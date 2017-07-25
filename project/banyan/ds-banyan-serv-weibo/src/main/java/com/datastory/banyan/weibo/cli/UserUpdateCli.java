package com.datastory.banyan.weibo.cli;

import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.flush_es.WeiboUserFlushESMR;
import com.datastory.banyan.weibo.process.AdvUserHBaseUpdater;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.quartz.SchedulerException;

/**
 * com.datastory.banyan.weibo.cli.UserUpdateCli
 *
 * @author lhfcws
 * @since 2017/4/11
 */
public class UserUpdateCli implements QuartzExecutor, CliRunner {
    private String startUpdateDate = DateUtils.getCurrentDateStr() + "000000";

    @Override
    public void execute() {
        System.out.println("【START】 ================== " + DateUtils.getCurrentPrettyTimeStr());
        updateAdv();
        flushEs();
        System.out.println("【END】   ================== " + DateUtils.getCurrentPrettyTimeStr());
    }

    private void updateAdv() {
        System.out.println("==================================================");
        System.out.println("=================== ADV UPDATE ===================");
        try {
            AdvUserHBaseUpdater.main(new String[] { "false" });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void flushEs() {
        System.out.println("==================================================");
        System.out.println("==================== FLUSH ES ====================");
        System.out.println("[FLUSH] Last startUpdateDate = " + startUpdateDate);
        String now = DateUtils.getCurrentTimeStr();

        String from = startUpdateDate;
        String to = now;
        startUpdateDate = now;
        try {
            System.out.println(String.format("[FLUSH] {from => to} : {%s => %s}", from, to));
            WeiboUserFlushESMR.main(new String[]{from, to});
            System.out.println("[FLUSH] Current startUpdateDate = " + startUpdateDate);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption("run", false, "not-quartz mode");
        options.addOption("start", true, "start updateDate");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine commandLine) {
        return true;
    }

    @Override
    public void start(CommandLine commandLine) {
        String start = commandLine.getOptionValue("start");
        if (start != null)
            startUpdateDate = start;
        System.out.println("[FLUSH] Current startUpdateDate = " + startUpdateDate);

        if (!commandLine.hasOption("run")) {
            try {
                QuartzJobUtils.createQuartzJob("0 0 15 * * ?", "WeiboUserFlushESMR$QCli", this);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        } else {
            execute();
        }
    }

    public static void main(String[] args) throws Exception {
        AdvCli.initRunner(args, "UserUpdateCli", new UserUpdateCli());
    }
}
