package com.datastory.banyan.validate.entity;

import org.apache.commons.lang.StringUtils;

/**
 * Created by abel.chan on 17/7/5.
 */
public enum ErrorLevel {
    ERROR_IGNORE_NULL,//异常可被忽略，并置null
    ERROR_DELETE;//异常需被删除

    public static ErrorLevel getErrorLevel(String value) {
        if (StringUtils.isEmpty(value)) return null;
        if (value.equalsIgnoreCase(ERROR_IGNORE_NULL.toString())) {
            return ERROR_IGNORE_NULL;
        } else if (value.equalsIgnoreCase(ERROR_DELETE.toString())) {
            return ERROR_DELETE;
        }
        return null;
    }

    public int compare(ErrorLevel errorLevel) {
        return this.compareTo(errorLevel);
    }
}
