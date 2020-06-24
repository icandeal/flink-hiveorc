package com.ycf.flink.orc

import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer
import org.apache.hadoop.fs.Path

import scala.reflect.ClassTag

/**
  * Created by yuchunfan on 2019/6/25
  */
class RowTimeBucketer[T: ClassTag](fieldName: String) extends Bucketer[T] with Serializable {
  override def getBucketPath(clock: Clock, path: Path, element: T) = {
    val method = implicitly[ClassTag[T]].runtimeClass.getMethod("get"
      + fieldName.replaceFirst(fieldName.substring(0,1),fieldName.substring(0,1).toUpperCase())
    )
    val bucketName = method.invoke(element).toString
    new Path(path, fieldName + "=" + bucketName)
  }
}