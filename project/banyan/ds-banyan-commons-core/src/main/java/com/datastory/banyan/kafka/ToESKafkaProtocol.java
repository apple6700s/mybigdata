package com.datastory.banyan.kafka;

import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import java.io.Serializable;

/**
 * com.datastory.banyan.kafka.ToESKafkaProtocol
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class ToESKafkaProtocol implements Serializable {
    private String pk;
    private String table;
    private String update_date;
    private String publish_date;

    public ToESKafkaProtocol(String pk, String table, String update_date, String publish_date) {
        this.pk = pk;
        this.table = table;
        this.update_date = update_date;
        this.publish_date = publish_date;
    }

    public ToESKafkaProtocol() {
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUpdate_date() {
        return update_date;
    }

    public void setUpdate_date(String update_date) {
        this.update_date = update_date;
    }

    public String getPublish_date() {
        return publish_date;
    }

    public void setPublish_date(String publish_date) {
        this.publish_date = publish_date;
    }

    @Override
    public String toString() {
        return "{" +
                "pk='" + pk + '\'' +
                ", table='" + table + '\'' +
                ", update_date='" + update_date + '\'' +
                ", publish_date='" + publish_date + '\'' +
                '}';
    }

    public String toJson() {
        return FastJsonSerializer.serialize(this);
    }

    public static ToESKafkaProtocol fromJson(String json) {
        return FastJsonSerializer.deserialize(json, ToESKafkaProtocol.class);
    }
}
