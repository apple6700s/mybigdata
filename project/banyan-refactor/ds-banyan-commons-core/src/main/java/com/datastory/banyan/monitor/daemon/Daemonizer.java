package com.datastory.banyan.monitor.daemon;

import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.sun.akuma.CLibrary;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.io.TmpFileInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * com.datastory.banyan.monitor.daemon.Daemonizer
 * 暂不用fork-join的传统daemon构造法，一个是java原生不支持，得调用glibc的jni；另一个是kill掉如果能连带kill掉daemon目前也是可以接受的。
 * 以后看看有没需求再改成fork-join吧
 *
 * @author lhfcws
 * @since 2017/3/14
 */
public class Daemonizer {
    private static final Logger LOG = Logger.getLogger(Daemonizer.class);
    private static final String SPARK_WEB_URL = "http://spark-rhino.datatub.com";
    private static final String NORMAL = "NORMAL";
    private static final String PILEUP = "PILEUP";
    private static final String STUCK = "STUCK";

    private String name;
    private String pid;
    private AtomicBoolean keepMon = new AtomicBoolean(true);

    public Daemonizer(String name, String pid) {
        this.name = name;
        this.pid = pid;
    }

    static String wget(String url) throws Exception {
        String output = RuntimeUtil.getPid() + System.currentTimeMillis() + new Random().nextInt(1000) + ".html";
        String cmd = "wget --http-user=admin --http-password=adminamkt  -O " + output + " '" + url + "'";
        LOG.info("[CMD] " + cmd);
        TmpFileInputStream in = null;
        try {
            Process process = Runtime.getRuntime().exec(new String[]{
                    "sh", "-c", cmd
            });
            int flag = process.waitFor();
            if (flag > 0) {
                throw new Exception("wget exit value " + flag);
            }

            in = new TmpFileInputStream(output);
            return IOUtils.toString(in);
        } finally {
            new File(output).delete();
            if (in != null)
                in.close();
        }
    }

    private void startProcessNExit(String name) throws Exception {
        LOG.info("==================== restart process");
        String cmd = String.format("sh start-%s.sh", name);
        LOG.info("[CMD] " + cmd);
        Process process = Runtime.getRuntime().exec(new String[]{
                "sh", "-c", cmd
        });
        int flag = process.waitFor();
        LOG.info("===================  restart process end, process flag = " + flag);
        System.out.flush();
        Thread.sleep(5000);
        LogManager.shutdown();
        keepMon.notify();
        System.exit(0);
    }

    static String getAppUrlByAppName(String name) throws Exception {
        String html = wget(SPARK_WEB_URL);
        if (!BanyanTypeUtil.valid(html))
            throw new RuntimeException("FAIL : wget " + SPARK_WEB_URL);
        String appUrl = null;
        Pattern p = Pattern.compile(String.format("<a[^>]+>%s</a>", name));
        Matcher matcher = p.matcher(html);
        while (matcher.find()) {
            String g = matcher.group();
            if (g == null) {
                appUrl = null;
                break;
            }
            if (g.contains("history"))
                continue;

            LOG.info("[GROUP] " + g);
            g = g.replace(name, "");
            g = g.replace("<a href=\"", "");
            g = g.replace("</a>", "");
            g = g.replace("\">", "");

            appUrl = g.trim();
            LOG.info("[AppUrl] " + appUrl);
            break;
        }
        return appUrl;
    }

    private String checkStuck() throws Exception {
        LOG.info("==================== check stuck");
        String status = NORMAL;
        String appUrl = getAppUrlByAppName(name);
        if (appUrl == null) {
            status = null;
        }

        if (status != null) {
            appUrl += "/streaming";
            String html = wget(appUrl);
            if (!BanyanTypeUtil.valid(html))
                throw new RuntimeException("FAIL : wget " + SPARK_WEB_URL);

            Pattern p = Pattern.compile("batch\\?id=[0-9]+");
            int phase = 0;
            int in_queue = 0;
            long diff = 0;
            for (String line : html.split("\n")) {
                if (line.contains("Active Batches"))
                    phase = 1;
                else if (line.contains("Completed Batches"))
                    phase = 2;

                if (phase == 0)
                    continue;

                Matcher matcher = p.matcher(line);
                if (matcher.find()) {
                    if (phase == 1)
                        in_queue++;
                    else {
                        String g = matcher.group();
                        LOG.info("[GROUP] " + g);
                        long batchTS = Long.valueOf(g.replace("batch?id=", ""));
                        long now = System.currentTimeMillis();
                        diff = now - batchTS;
                        LOG.info(String.format("[Diff] %d = %d - %d", diff, now, batchTS));
                        break;
                    }
                }
            }

            if (in_queue == 0 && diff > 10 * 60 * 1000)
                status = STUCK;
//            else if (in_queue > 0 && diff > 2 * 3600 * 1000)
//                status = PILEUP;
        }

        LOG.info("[STATUS] " + status);
        LOG.info("==================== finish check stuck");
        return status;
    }

    private void monitor(String name, String pid) throws Exception {
        boolean exists = RuntimeUtil.existProcess(pid);
        if (!exists) {
            keepMon.set(false);
            LOG.error("process " + pid + " is dead");
            startProcessNExit(name);
        } else {
            String status = checkStuck();
            LOG.info("process status = " + status);
            if (NORMAL.equals(status))
                return;

            Process process = null;
            keepMon.set(false);

            // deprecated PILEUP
            if (PILEUP.equals(status)) {
                new ReceiverKill().kill(name);
                process = Runtime.getRuntime().exec(new String[]{
                        "sh", "-c", "kill " + pid
                });
                process.waitFor();
                LOG.info("kill " + pid);
            } else if (STUCK.equals(status)) {
                SparkUtil.stopStreamingContextForce(name, pid, 15);

//                Thread.sleep(2 * 60 * 1000);    // 2min
//                process = Runtime.getRuntime().exec(new String[]{
//                        "sh", "-c", "kill -9 " + pid
//                });
//                process.waitFor();
//                LOG.info("kill -9 " + pid);
            } else {
                process = Runtime.getRuntime().exec(new String[]{
                        "sh", "-c", "kill " + pid
                });
                process.waitFor();

//                Thread.sleep(2 * 60 * 1000);    // 2min
//                process = Runtime.getRuntime().exec(new String[]{
//                        "sh", "-c", "kill -9 " + pid
//                });
//                process.waitFor();
//                LOG.info("kill -9 " + pid);
            }

            startProcessNExit(name);
        }
    }

    private void loop() throws InterruptedException {
        LOG.info("*************************************");
        LOG.info("*************************************");
        Thread.sleep(10 * 60 * 1000); // sleep 5 min for initialization
        while (keepMon.get()) {
            try {
                Thread.sleep(60 * 1000);
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            monitor(name, pid);
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    }
                });
                thread.start();
                thread.join(60 * 1000);
                thread.stop();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        keepMon.wait();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        String name = args[0];
        String pid = args[1];

        CLibrary.LIBC.setsid();
        Daemonizer daemonizer = new Daemonizer(name, pid);
        daemonizer.loop();

        System.out.println("[PROGRAM] Program exited.");
    }
}
