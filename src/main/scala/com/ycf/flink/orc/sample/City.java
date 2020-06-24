package com.ycf.flink.orc.sample;

import java.io.Serializable;

/**
 * Created by yuchunfan on 2019/7/11.
 */
public class City implements Serializable {
    private Long ref;
    private Long city_id;
    private String city_name;
    private String c_date;

    public City(Long ref, Long city_id, String city_name, String c_date) {
        this.ref = ref;
        this.city_id = city_id;
        this.city_name = city_name;
        this.c_date = c_date;
    }

    public Long getRef() {
        return ref;
    }

    public void setRef(Long ref) {
        this.ref = ref;
    }

    public Long getCity_id() {
        return city_id;
    }

    public void setCity_id(Long city_id) {
        this.city_id = city_id;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getC_date() {
        return c_date;
    }

    public void setC_date(String c_date) {
        this.c_date = c_date;
    }

    @Override
    public String toString() {
        return "City{" +
                "ref=" + ref +
                ", city_id=" + city_id +
                ", city_name='" + city_name + '\'' +
                ", c_date='" + c_date + '\'' +
                '}';
    }
}
