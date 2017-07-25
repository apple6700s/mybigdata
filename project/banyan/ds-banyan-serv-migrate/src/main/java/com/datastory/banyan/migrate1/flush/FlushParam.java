package com.datastory.banyan.migrate1.flush;

import java.io.Serializable;

/**
 * com.datastory.banyan.migrate1.flush.FlushParam
 *
 * @author lhfcws
 * @since 2017/6/28
 */
public class FlushParam implements Serializable {
    private String type;
    private String hbTable;
    private String oriTable;

    public FlushParam(String type, String hbTable) {
        this.type = type;
        this.hbTable = hbTable;
    }

    public FlushParam(String type, String hbTable, String oriTable) {
        this.type = type;
        this.hbTable = hbTable;
        this.oriTable = oriTable;
    }

    public FlushParam() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHbTable() {
        return hbTable;
    }

    public void setHbTable(String hbTable) {
        this.hbTable = hbTable;
    }

    public String getOriTable() {
        return oriTable;
    }

    public void setOriTable(String oriTable) {
        this.oriTable = oriTable;
    }
}
