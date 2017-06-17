package com.datastory.banyan.monitor.utils;

import com.sun.akuma.JavaVMArguments;
import com.yeezhao.commons.util.RuntimeUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * com.datastory.banyan.monitor.utils.StreamingConsumerDaemon
 *
 * @author lhfcws
 * @since 2017/1/16
 */
public class StreamingConsumerDaemon {
    public static final String PYFILE = "daemon.py";
    public static final String[] CMDS =new String[] {
            "sh", "-c", ""
    };
//    public static final String CMD = "nohup python daemon.py ";
    public static final String CMD = "sh start-daemon.sh ";

    public static void daemon(String name) throws Exception {
        String pid = RuntimeUtil.PID;
        JavaVMArguments arguments = JavaVMArguments.current();
        String args = StringUtils.join(arguments, " ");

//        String tmpFn = "/tmp/" + name + "." + pid;
//        FileOutputStream os = new FileOutputStream(tmpFn);
//        IOUtils.write(args.getBytes(), os);
//        os.flush();
//        os.close();

        String cmd = String.format("%s %s %s", CMD, name, pid);

        System.out.println("[DAEMON] Launch daemon:  " + cmd);

        String[] cmds = new String[] {
                "sh", "-c", cmd
        };

        Process process = new ProcessBuilder().command(cmds).start();
        process.waitFor();
    }
}
