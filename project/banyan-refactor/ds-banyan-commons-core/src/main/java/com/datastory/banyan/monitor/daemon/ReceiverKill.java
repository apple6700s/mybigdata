package com.datastory.banyan.monitor.daemon;

import com.datastory.banyan.utils.BanyanTypeUtil;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * com.datastory.banyan.monitor.daemon.ReceiverKill
 *
 * @author lhfcws
 * @since 2017/3/23
 */
public class ReceiverKill {
    private static Logger LOG = Logger.getLogger(ReceiverKill.class);

    public void kill(String appName) throws Exception {
        String appUrl = Daemonizer.getAppUrlByAppName(appName);
        String stageUrl = appUrl + "/stages";
        String html = Daemonizer.wget(stageUrl);
        if (!BanyanTypeUtil.valid(html))
            throw new RuntimeException("FAIL : wget " + stageUrl );

        boolean find = false;
        String killUrl = null;
        for (String line : html.split("\n")) {
            if (line.contains("Streaming job running receiver"))
                find = true;
            if (find && line.contains("/stages/stage/kill/?id=")) {
                Pattern p = Pattern.compile("id=[0-9]+");
                Matcher matcher = p.matcher(line);
                if (matcher.find()) {
                    String idStr = matcher.group();
                    killUrl = stageUrl + "/stage/kill/?" + idStr + "&terminate=true";
                    break;
                }
            }
        }

        if (killUrl != null) {
            try {
                Daemonizer.wget(killUrl);
            } catch (Exception ignore) {}
            LOG.info("killed : "  + killUrl);
        } else
            throw new Exception("Cannot find receiver kill url on Spark UI.");
    }

    public static void main(String[] args) throws Exception {
        String appName = args[0];
        new ReceiverKill().kill(appName);
    }
}
