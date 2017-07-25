package com.datastory.banyan.monitor;

import com.datastory.banyan.monitor.reporter.AliveFalconMetricReporter;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * TODO: Maybe later we can use dt-commons-configs to register into Owl.
 * com.datatub.rhino.monitor.AliveMonCli
 *
 * @author lhfcws
 * @since 2016/11/7
 */
public abstract class AliveMonCli implements CliRunner {
    private static final String _ORG = "yeezhao";
    private static final String _APP = "datastory";
    private String _SERVNAME = this.getClass().getSimpleName();
    protected AliveFalconMetricReporter aliveFalconMetricReporter = new AliveFalconMetricReporter(_SERVNAME);

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption(AdvCli.CLI_PARAM_H, false, "help");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine cmd) {
        return true;
    }

    @Override
    public void start(CommandLine cmd) {
        try {
            aliveFalconMetricReporter.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        _start();
    }

    public abstract void _start();
}
