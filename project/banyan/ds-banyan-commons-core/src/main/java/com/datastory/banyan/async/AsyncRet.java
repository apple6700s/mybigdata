package com.datastory.banyan.async;

import java.io.Serializable;

/**
 * @author lhfcws
 * @since 16/1/12.
 */
public class AsyncRet implements Serializable {
    public AsyncRet(int status, Object value) {
        this.status = status;
        this.value = value;
    }

    public AsyncRet(int status, String msg, Object value) {
        this.status = status;
        this.value = value;
        this.msg = msg;
    }

    public AsyncRet() {
    }

    public AsyncRet(boolean flag) {
        this();
        this.status = flag ? 1: -1;
    }


    public static AsyncRet fail(String msg, Object value) {
        return new AsyncRet(FAIL, msg, value);
    }

    public static AsyncRet fail(String msg) {
        return new AsyncRet(FAIL, msg, "");
    }

    public static AsyncRet success(Object value) {
        return new AsyncRet(SUCCESS, "", value);
    }

    public static AsyncRet success() {
        return new AsyncRet(SUCCESS, "", "");
    }

    public boolean isBadStatus() {
        return status < 0;
    }

    public int status;
    public Object value;
    public String msg = "";

    public static final int SUCCESS = 1;
    public static final int WAIT = 0;
    public static final int FAIL = -1;
    public static final int TIMEOUT = -2;

}
