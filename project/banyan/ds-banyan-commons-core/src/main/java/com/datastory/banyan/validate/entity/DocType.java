package com.datastory.banyan.validate.entity;

/**
 * Created by abel.chan on 17/7/19.
 */
public enum DocType {
    NORMAL, ERROR_IGNORE, ERROR_DELETE, ALL;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
