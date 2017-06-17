package com.datastory.banyan.monitor;

import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * com.datastory.banyan.monitor.Status
 *
 * @author lhfcws
 * @since 2016/12/29
 */
@Deprecated
public class Status {
    protected String status = MonConsts.SUCCESS;
    protected Object payload = null;

    public Status(String status) {
        this.status = status;
    }

    public Status(String status, Object payload) {
        this.status = status;
        this.payload = payload;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public static Status buildJDBCStatus(int r) {
        if (isJDBCSuccess(r))
            return new Status(MonConsts.SUCCESS);
        else
            return new Status(MonConsts.FAIL);
    }

    public static boolean isJDBCSuccess(int r) {
        return r >= 0 || r == SUCCESS_NO_INFO;
    }

    public static boolean isHBaseSuccess(int r) {
        return r > 0;
    }
}
