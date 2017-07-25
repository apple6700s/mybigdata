package com.datastory.banyan.utils;

import com.datastory.banyan.base.ConfUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * com.datastory.banyan.utils.Emailer
 *
 * @author lhfcws
 * @since 2016/11/5
 */
public class Emailer implements Serializable {
    private static Logger LOG = Logger.getLogger(EmailUtil.class);
    private static final String DEFAULT_CHARSET = "UTF-8";
    private static Configuration conf = ConfUtil.resourceConf("email-config.xml");

    public static SimpleEmail buildSimpleEmail() throws EmailException {
        SimpleEmail email = new SimpleEmail();
        email.setHostName(conf.get("alarm.sender.smtp"));
        email.setSSL(true);
        email.setAuthentication(conf.get("alarm.sender.username"), conf.get("alarm.sender.password"));
        email.setFrom(conf.get("alarm.sender.username"), "BANYAN");
        email.addTo(StringUtils.split(conf.get("alarm.receiver"), ";"));
        email.setCharset(DEFAULT_CHARSET);
        return email;
    }

    public static void sendMonitorEmail(String title, String content) {
        if (StringUtil.isNullOrEmpty(content))
            return;
        try {
            SimpleEmail email = buildSimpleEmail();
            email.setSubject("[监控] Banyan ETL监控, " + title);
            email.setMsg(DateUtils.getCurrentPrettyTimeStr() + "\n" + content + "\n");
            email.send();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static void sendMonitorEmail(String content) {
        if (StringUtil.isNullOrEmpty(content))
            return;
        try {
            SimpleEmail email = buildSimpleEmail();
            email.setSubject("[监控] Banyan ETL监控");
            email.setMsg(DateUtils.getCurrentPrettyTimeStr() + "\n" + content + "\n");
            email.send();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static void sendAlarmEmail(String content) {
        if (StringUtil.isNullOrEmpty(content))
            return;
        try {
            SimpleEmail email = buildSimpleEmail();
            email.setSubject("【告警】 Banyan ETL监控告警！！！");
            email.setMsg(DateUtils.getCurrentPrettyTimeStr() + "\n" + content + "\n");
            email.send();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public static void sendExceptionEmail(String content) {
        if (StringUtil.isNullOrEmpty(content))
            return;
        try {
            SimpleEmail email = buildSimpleEmail();
            email.setSubject("【异常】 Banyan ETL异常报告");
            email.setMsg(DateUtils.getCurrentPrettyTimeStr() + "\n" + content + "\n");
            email.send();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    /**
     * Test main
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        Emailer.sendMonitorEmail("Test banyan monitor email.");
        Emailer.sendAlarmEmail("Test banyan alarm email.");
        Emailer.sendExceptionEmail("Test banyan exception email.");
        System.out.println("[PROGRAM] Program exited.");
    }
}
