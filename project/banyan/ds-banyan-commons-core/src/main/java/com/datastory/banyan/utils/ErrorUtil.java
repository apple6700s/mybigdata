package com.datastory.banyan.utils;

import com.alibaba.fastjson.JSONObject;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;

/**
 * com.datatub.rhino.utils.ErrorUtil
 * Handle the exceptions or errors (send emails / send msg / singleWrite to log center , etc.)
 *
 * @author lhfcws
 * @since 2016/11/4
 */
public class ErrorUtil {
    public static final String HOST = RuntimeUtil.Hostname;
    public static final String PNAME = ManagementFactory.getRuntimeMXBean().getName();

    public static final Logger _LOG = Logger.getLogger(ErrorUtil.class);

    public static void print(String prefix, Object msg) {
        System.out.println(prefix + " " + RuntimeUtil.PID + " " + msg);
    }

    public static void simpleError(Throwable e) {
        _LOG.error(PNAME + "@" + HOST + ", " + e.getMessage() + " : " + extractStack(e.getStackTrace()));
    }

    public static void simpleError(String msg, Throwable e) {
        _LOG.error(PNAME + "@" + HOST + ", " + msg + ". " + e.getMessage() + " : " + extractStack(e.getStackTrace()));
    }

    public static void simpleError(Logger LOG, Throwable e) {
        LOG.error(PNAME + "@" + HOST + ", " + e.getMessage() + " : " + extractStack(e.getStackTrace()));
    }

    public static void infoConsumingProgress(Logger LOG, String prefix, JSONObject jsonObject, String others) {
        String msg = prefix + " uDate=" + jsonObject.getString("update_date") + ", pDate=" + jsonObject.getString("publish_date") + ", taskId=" + jsonObject.getString("taskId");
        if (!StringUtil.isNullOrEmpty(others))
            msg = msg + ", " + others;
        LOG.info(msg);
    }

    public static void infoConsumingProgress(Logger LOG, String prefix, String uDate, String pDate, String others) {
        String msg = prefix + " uDate=" + uDate + ", pDate=" + pDate;
        if (!StringUtil.isNullOrEmpty(others))
            msg = msg + ", " + others;
        LOG.info(msg);
    }

    public static void error(Throwable e) {
        error(_LOG, e, "");
    }

    public static void error(Logger LOG, Throwable e) {
        error(LOG, e, "");
    }

    public static void error(Logger LOG, Throwable e, String msg) {
        LOG.error(PNAME + "@" + HOST + ", " + e.getMessage() + ", " + msg, e);

        // send email
        Exception ew = new ExceptionWrapper(e, msg);
        Emailer.sendExceptionEmail(ErrorUtil.e2s(ew));
    }

    public static String e2s(Throwable e) {
        StringBuilder sb = new StringBuilder();
        sb.append(PNAME + "@" + HOST + ", " + e.getMessage());
        for (StackTraceElement stackTraceElement : e.getStackTrace()) {
            sb.append("\n").append(stackTraceElement.toString());
        }
        return sb.toString();
    }

    public static class ExceptionWrapper extends Exception {
        public ExceptionWrapper(Throwable e, String msg) {
            super(e.getMessage() + ", " + msg);
            this.setStackTrace(e.getStackTrace());
        }
    }

    private static String extractStack(StackTraceElement[] arr) {
        if (arr == null || arr.length == 0)
            return "";
        else if (arr.length == 1)
            return arr[0].toString();
        else if (arr.length == 2)
            return arr[0].toString() + " | " + arr[1].toString();
        else
            return arr[0].toString() + " | " + arr[1].toString() + " | " + arr[2].toString();
    }
}
