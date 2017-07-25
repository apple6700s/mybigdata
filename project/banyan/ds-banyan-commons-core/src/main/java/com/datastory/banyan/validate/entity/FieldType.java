package com.datastory.banyan.validate.entity;

/**
 * Created by abel.chan on 17/7/19.
 */
public enum FieldType {
    NORMAL, NULL, ZERO, ERROR;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
