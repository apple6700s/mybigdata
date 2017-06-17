package com.datastory.banyan.retry.record;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.FileSystemHelper;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * com.datastory.banyan.retry.log.HDFSLog
 *
 * @author lhfcws
 * @since 16/12/7
 */

public class HDFSLog {
    static {
        IdleHDFSLogChecker.start();
    }

    private static ConcurrentHashMap<String, HDFSLog> _logs = new ConcurrentHashMap<>();

    public static HDFSLog get(String key) {
        if (!_logs.containsKey(key)) {
            _logs.putIfAbsent(key, new HDFSLog(RhinoETLConfig.getInstance(), key));
        }
        return _logs.get(key);
    }

    private Configuration conf;
    private FSDataOutputStream os = null;
    private String root;
    private String key;
    private volatile long lastLogTime = System.currentTimeMillis();

    private static final String DAYFORMAT = "yyyyMMdd";
    private static final String TIMEFORMAT = "yyyy-MM-dd HH:mm:ss";

    private static String getDateStr(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date());
    }

    private HDFSLog(Configuration conf, String key) {
        this.conf = conf;
        this.key = key;

        root = conf.get("retry.log.path", "/tmp");

        FileSystemHelper fs = FileSystemHelper.getInstance(conf);
        try {
            if (!fs.existFile(root))
                fs.mkdirs(root);
            initOutputHandler();
        } catch (Exception ignore) {
        }

    }

    private void initOutputHandler() throws IOException, InterruptedException {
        String dir = root + "/" + getDateStr(DAYFORMAT) + "/" + key;
        String path = dir + "/" + genUniqFn();
        FileSystemHelper fs = FileSystemHelper.getInstance(conf);
        try {
            if (!fs.existFile(dir))
                fs.mkdirs(dir);
            os = (FSDataOutputStream) fs.getHDFSFileOutputStream(path);
        } catch (Exception ignore1) {
            os = (FSDataOutputStream) fs.getHDFSFileAppendOutputStream(path);
        }
    }

    private void closeHandler() throws IOException {
        if (os != null) {
            os.flush();
            os.close();
            os = null;
        }
    }

    private String genUniqFn() {
        return RuntimeUtil.Hostname + "_" + RuntimeUtil.PID + ".log";
    }

    public void log(Object msg) {
        lastLogTime = System.currentTimeMillis();
        String outMsg = (msg + "").trim();
        if (StringUtil.isNullOrEmpty(outMsg))
            return;
        outMsg = outMsg + "\n";
//                outMsg = StringEscapeUtils.escapeJava(outMsg);
        byte[] bs = Bytes.toBytes(outMsg);

        synchronized (this) {
            try {
                if (os == null)
                    initOutputHandler();
                try {
                    os.write(bs);
                } catch (ClosedChannelException cce) {
                    if (this.os != null) {
                        try {
                            closeHandler();
                        } catch (Exception ignore) {
                        }
                    }
                    initOutputHandler();
                    os.write(bs);
                }
                os.flush();
            } catch (Exception e) {
                ErrorUtil.simpleError(e);
            }
        }
    }

    public void flush() throws IOException {
        if (os != null)
            synchronized (this) {
                if (os != null)
                    os.flush();
            }
    }

    public void info(Object msg) {
        this.log("[INFO] " + getDateStr(TIMEFORMAT) + " " + msg);
    }

    public void error(Object msg) {
        this.log("[ERROR] " + getDateStr(TIMEFORMAT) + " " + msg);
    }

    public void error(Throwable e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement ste : e.getStackTrace()) {
            sb.append("\n").append(ste.toString());
        }
        this.log("[ERROR] " + getDateStr(TIMEFORMAT) + " " + e.getMessage() + sb.toString());
    }

    private static class IdleHDFSLogChecker implements QuartzExecutor {
        public static final String CRON = "0 */20 * * * ?";
        public static final long MAX_TIMEOUT = 10 * 60 * 1000;

        @Override
        public void execute() {
            List<HDFSLog> logs = new LinkedList<>(_logs.values());
            for (HDFSLog hdfsLog : logs) {
                long now = System.currentTimeMillis();
                if (now - hdfsLog.lastLogTime > MAX_TIMEOUT)
                    synchronized (hdfsLog) {
                        if (now - hdfsLog.lastLogTime > MAX_TIMEOUT) {
                            try {
                                hdfsLog.closeHandler();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
            }
        }

        private void _start() {
            try {
                QuartzJobUtils.createQuartzJob(CRON, "IdleHDFSLogChecker", this);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }

        public static void start() {
            new IdleHDFSLogChecker()._start();
        }
    }
}
