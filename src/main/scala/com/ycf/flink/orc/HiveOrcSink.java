package com.ycf.flink.orc;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by yuchunfan on 2019/7/16.
 */
public class HiveOrcSink<T> extends RichSinkFunction<T>
        implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {
    private static final Long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger("HiveOrcSink");

    private static final Long DEFAULT_BATCH_SIZE = 1024L * 500L;
    private static final Long DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS = 10 * 1000L;
    private static final Long DEFAULT_MAX_FILE_OPEN_THRESHOLD_MS = 30 * 1000L;
    private static final String HDFS_TMP_DIR_PREFIX = "/flink/orc/tmp/";
    private static final String HDFS_PENDING_DIR_PREFIX = "/flink/orc/pending/";
    private static final String HIVE_DEFAULT_WAREHOUSE = "/user/hive/warehouse";

    private static String tmp_dir_prefix = HDFS_TMP_DIR_PREFIX;
    private static String pending_dir_prefix = HDFS_PENDING_DIR_PREFIX;

    private static transient org.apache.hadoop.conf.Configuration fsConf;
    private static transient Clock clock;
    private static transient ProcessingTimeService processingTimeService;
    private static transient FileSystem fs;

    private transient ListState<State<T>> restoredBucketStates;
    private transient State<T> state;

    private String tableName;
    private String hiveWarehouse = HIVE_DEFAULT_WAREHOUSE;
    private Long batchSize = DEFAULT_BATCH_SIZE;
    private Long inactiveBucketCheckInterval = DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS;
    private Long maxFileOpenThreshold = DEFAULT_MAX_FILE_OPEN_THRESHOLD_MS;
    private String hdfsPath;
    private Bucketer<T> bucketer;
    private Class<T> javaEntity;

    public HiveOrcSink(Class<T> javaEntity, String tableName) {
        log.info("*************************************  JAVA INIT  *************************************");
        this.tableName = tableName;
        this.javaEntity = javaEntity;
        hdfsPath = hiveWarehouse.endsWith("/") ? hiveWarehouse + tableName: hiveWarehouse + "/" + tableName;
    }

    public HiveOrcSink<T> setHiveWarehouse(String hiveWarehouse) {
        this.hiveWarehouse = hiveWarehouse;
        hdfsPath = hiveWarehouse.endsWith("/") ? hiveWarehouse + tableName: hiveWarehouse + "/" + tableName;
        return this;
    }

    public HiveOrcSink<T> setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public HiveOrcSink<T> setInactiveBucketCheckInterval(Long inactiveBucketCheckInterval) {
        this.inactiveBucketCheckInterval = inactiveBucketCheckInterval;
        return this;
    }

    public HiveOrcSink<T> setMaxFileOpenThreshold(Long maxFileOpenThreshold) {
        this.maxFileOpenThreshold = maxFileOpenThreshold;
        return this;
    }

    /**********************************************  State  *******************************************/
    /**************************************************************************************************/
    public final class State<T> implements Serializable {
        private final Map<Path, BucketState<T>> bucketStateMap = new HashMap<>();

        public int subTaskId;

        public State() {
        }

        public State(int subTaskId) throws IOException {
            this.subTaskId = subTaskId;
        }

        public void writeInBucket(T element, Path bucketPath, Long processTime) throws IOException {
            BucketState<T> bucketState = bucketStateMap.getOrDefault(bucketPath, null);
            if (bucketState == null) {
                bucketState = new BucketState<>(bucketPath, processTime, subTaskId);
                fs.mkdirs(bucketPath);
                bucketStateMap.put(bucketPath, bucketState);
            }
            bucketState.write(element, processTime);
        }

        public void close(Long processTime) throws IOException {
            for (BucketState<T> bucketState : bucketStateMap.values()) {
                bucketState.flush(processTime);
                bucketState.persist();
            }
        }

        public void persist() throws IOException {
            for (BucketState<T> bucketState : bucketStateMap.values()) {
                bucketState.persist();
            }
        }

        public void recovery() throws Exception {
            for (BucketState<T> bucketState : bucketStateMap.values()) {
                bucketState.recovery();
            }
        }

        public void checkBucket(Long currentTime) throws IOException {
            for (BucketState<T> bucketState : bucketStateMap.values()) {
                log.info("TID:" + subTaskId + "> " + "Check Interval => " + bucketState.toString());
                if (currentTime - bucketState.getLastUpdateTime() > maxFileOpenThreshold
                        && bucketState.hasWorkingFile()) {
                    bucketState.flush(currentTime);
                }
            }
        }

        public Map<Path, BucketState<T>> getBucketStateMap() {
            return bucketStateMap;
        }

        public int getSubTaskId() {
            return subTaskId;
        }

        public void setSubTaskId(int subTaskId) {
            this.subTaskId = subTaskId;
        }
    }
    /**************************************************************************************************/

    /*******************************************  BucketState  ****************************************/
    /**************************************************************************************************/
    public final class BucketState<T> implements Serializable {
        public Path bucketPath;
        public int subTaskId;

        public Long lastUpdateTime;
        public DataFile<T> workingFile;

        public BucketState() {
        }

        public BucketState(Path bucketPath, Long lastUpdateTime, int subTaskId) {
            this.bucketPath = bucketPath;
            this.lastUpdateTime = lastUpdateTime;
            this.subTaskId = subTaskId;
        }

        private final List<DataFile<T>> pendingFileList = new ArrayList<>();


        public void recovery() throws Exception {
            try {
                log.info("TID:" + subTaskId + "> " + "Bucket " + bucketPath.getName() + " is recovering !!");
                if (!fs.exists(bucketPath)) {
                    fs.mkdirs(bucketPath);
                }
                for (DataFile<T> dataFile : pendingFileList) {
                    dataFile.clear();
                }
                pendingFileList.clear();
                if (workingFile != null) {
                    workingFile.recovery();
                }
            } catch (Exception e) {
                log.error("TID:" + subTaskId + "> " + "BucketState.recovery ERROR !!",e);
                throw e;
            }
        }

        public boolean hasWorkingFile() {
            return workingFile != null;
        }

        public void write(T element, Long processTime) throws IOException {
//            lastUpdateTime = processTime;         // 注释此处，则一个文件固定打开 maxFileOpenThreshold 时间会被关闭
            if (workingFile == null) {
                Random random = new Random();
                workingFile = new DataFile<>(bucketPath, "part_" + subTaskId + "_" +
                    processTime + "_"+ Math.abs(random.nextLong()) + ".orc", subTaskId);
            }

            workingFile.write(element);

            if (workingFile.getPos() >= batchSize * 10) {
                log.info("TID:" + subTaskId + "> " + "Flush file in " + workingFile.getPos()+ " bytes >= "+ batchSize + " * 10 bytes !!");
                flush(processTime);
            }
        }

        public void flush(Long processTime) throws IOException {
            if (workingFile != null) {
                workingFile.close();
                pendingFileList.add(workingFile);
                lastUpdateTime = processTime;
                workingFile = null;
            }
        }

        public Long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void persist() throws IOException {
            if (!fs.exists(bucketPath)) {
                fs.mkdirs(bucketPath);
            }
            if (pendingFileList.size() > 0) {
                log.info("TID:" + subTaskId + "> " + "Persisting " + pendingFileList.size() + " files in " + bucketPath.toString() + " !!!");
                for (DataFile<T> dataFile : pendingFileList) {
                    dataFile.moveToBucket();
                }
                pendingFileList.clear();
            }
        }

        @Override
        public String toString() {
            return "BucketState{" +
                    "bucketPath=" + bucketPath +
                    ", subTaskId=" + subTaskId +
                    ", lastUpdateTime=" + lastUpdateTime +
                    ", hasWorkingFile=" + hasWorkingFile() +
                    ", currentDataSize=" + (workingFile == null? 0: workingFile.dataBuffer.size())+
                    ", currentDataBytes=" + (workingFile == null? 0: workingFile.dataSize)+
                    ", pendingFileListSize=" + pendingFileList.size() +
                    '}';
        }
    }
    /**************************************************************************************************/


    /*********************************************  DataFile  *****************************************/
    /**************************************************************************************************/
    public final class DataFile<T> implements Serializable {
        public Path filePath;
        public Path tmpPath;
        public Path pendingPath;
        public Integer subTaskId;
        public Long dataSize = 0L;
        private final List<T> dataBuffer = new ArrayList<>();

        private transient org.apache.hadoop.hive.ql.io.orc.Writer writer = null;

        public DataFile(Path bucketPath, String fileName, Integer subTaskId) {
            this.subTaskId = subTaskId;
            filePath = new Path(bucketPath, fileName);
            String parent = bucketPath.getParent().getName() + "/" + bucketPath.getName();
            tmpPath = new Path(tmp_dir_prefix + parent + "/" + fileName);
            pendingPath = new Path(pending_dir_prefix + parent + "/" + fileName);
        }

        public void write(T element) throws IOException {
            if (writer == null)
                initWriter();
            writer.addRow(element);
            dataBuffer.add(element);
            dataSize += ObjectSizeCalculator.getObjectSize(element);
        }

        private void initWriter() throws IOException {

            if (fs.exists(tmpPath)) {
                fs.delete(tmpPath, true);
            }
            if (fs.exists(pendingPath)) {
                fs.delete(pendingPath, true);
            }

            log.info("TID:" + subTaskId + "> " + "Init writer tmpPath => " + tmpPath);
            log.info("TID:" + subTaskId + "> " + "Init writer pendingPath => " + pendingPath);
            if (!fs.exists(tmpPath.getParent())) {
                fs.mkdirs(tmpPath.getParent());
            }
            if (!fs.exists(pendingPath.getParent())) {
                fs.mkdirs(pendingPath.getParent());
            }

            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(
                javaEntity,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA
            );

            writer = OrcFile.createWriter(tmpPath, OrcFile.writerOptions(fs.getConf()).inspector(inspector));
        }

        public Long getPos() {
            return this.dataSize;
        }

        public void close() throws IOException {
            writer.close();
            fs.rename(tmpPath, pendingPath);
            log.info("TID:" + subTaskId + "> " + "Write " + dataSize + " Bytes data !!");
            dataSize = 0l;
            dataBuffer.clear();
        }

        public void recovery() throws IOException {
            initWriter();
            log.info("TID:" + subTaskId + "> " + "Recover data number => " + dataBuffer.size() +" slices !!");
            for (T e : dataBuffer) {
                writer.addRow(e);
            }
        }

        public void clear() throws IOException {
            fs.delete(tmpPath, true);
            fs.delete(pendingPath, true);
        }

        public void moveToBucket() throws IOException {
            log.info("TID:" + subTaskId + "> " + "Move Peding to filePath =>" + filePath);
            fs.rename(pendingPath, filePath);
        }
    }

    /**************************************************************************************************/

    private void initFileSystem(Integer subTaskId) throws IOException {
        log.info("TID:" + subTaskId + "> " + "### init filesystem ###");
        if (fsConf == null) {
            fsConf = new org.apache.hadoop.conf.Configuration();
        }

        log.info("TID:" + subTaskId + "> " + "***  hdfsPath => "+ hdfsPath + "***");

        if (fs == null) {
            try {
                String disableCacheName = String.format("fs.%s.impl.disable.cache", new Path(hdfsPath).toUri().getScheme());
                fsConf.setBoolean(disableCacheName, true);
                fs = new Path(hdfsPath).getFileSystem(fsConf);
            } catch (IOException e) {
                log.error("TID:" + subTaskId + "> initFileSystem ERROR !!", e);
                throw e;
            }
        }
    }

    public HiveOrcSink<T> setBucketer(Bucketer<T> bucketer) {
        this.bucketer = bucketer;
        return this;
    }

    @Override
    public void close() throws Exception {
        synchronized (state) {
            log.warn("TID:" + getRuntimeContext().getIndexOfThisSubtask() + "> " + "!!!  On Sink Close !!!");
            Long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
            state.close(currentProcessingTime);
            super.close();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Integer subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        log.info("TID:" + subTaskId + "> " + "!!!  in initializeState  !!!");

        processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        this.clock = () -> processingTimeService.getCurrentProcessingTime();

        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor listStateDescriptor = new ListStateDescriptor<>(
            "dataBuffer", TypeInformation.of(new TypeHint<State<T>>() {
        }));

        restoredBucketStates = stateStore.getListState(listStateDescriptor);

        Long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        initFileSystem(subTaskId);

        if (context.isRestored()) {
            log.info("TID:" + subTaskId + "> " + "!!!  is restored  !!!");
            Iterator<State<T>> itr = restoredBucketStates.get().iterator();
            if (itr.hasNext()) {
                state = itr.next();
                state.recovery();
            } else {
                log.info("TID:" + subTaskId + "> " + "!!!  recovery failed  !!!");
                state = new State<>(getRuntimeContext().getIndexOfThisSubtask());
            }
        } else {
            log.info("TID:" + subTaskId + "> " + "!!!  restart  !!!");
            state = new State<>(getRuntimeContext().getIndexOfThisSubtask());

        }
        processingTimeService.registerTimer(
            currentProcessingTime + inactiveBucketCheckInterval,
            this
        );
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        synchronized (state) {
            if (bucketer == null) {
                throw new IllegalStateException("未定义 Bucketer !!");
            }
            Path bucketPath = bucketer.getBucketPath(clock, new Path(hdfsPath), value);
            Long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

            state.writeInBucket(value, bucketPath, currentProcessingTime);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (state) {
            Integer subTaskId = getRuntimeContext().getIndexOfThisSubtask();
            log.info("TID:" + subTaskId + "> " + "!!!  snapshot  !!!");
            restoredBucketStates.clear();
            restoredBucketStates.add(state);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (state) {
            Integer subTaskId = getRuntimeContext().getIndexOfThisSubtask();
            log.info("TID:" + subTaskId + "> " + "!!!  chk complete  !!!");
            state.persist();
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        synchronized (state) {
            Long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
            state.checkBucket(currentProcessingTime);
            processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
        }
    }
}
