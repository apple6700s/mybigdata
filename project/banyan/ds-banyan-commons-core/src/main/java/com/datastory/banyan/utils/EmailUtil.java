package com.datastory.banyan.utils;

import org.apache.commons.mail.SimpleEmail;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;

/**
 * Created by liny on 2016/9/23.
 */
public class EmailUtil {

    private static Logger LOG = Logger.getLogger(EmailUtil.class);
    // 编码
    private static final String DEFAULT_CHARSET = "UTF-8";
    // 邮件设置
    private static final String MAILER_HOSTNAME = "smtp.exmail.qq.com";
    private static final String MAILER_USERNAME = "report@hudongpai.com";
    private static final String MAILER_PASSWORD = "yeezhao168";
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static void sendMail(String[] toEmail, String title, String content) {
        try {
            SimpleEmail email = new SimpleEmail();
            email.setHostName(MAILER_HOSTNAME);
            email.setSSL(true);
            email.setAuthentication(MAILER_USERNAME, MAILER_PASSWORD);
            email.setFrom(MAILER_USERNAME);
            email.addTo(toEmail);
            email.setSubject(title);
            email.setCharset(DEFAULT_CHARSET);
            email.setContent(content, "text/html;charset=UTF-8");
            email.send();
        } catch (Exception e1) {
//            e1.printStackTrace();
            LOG.error(e1.getMessage());
        }
    }

    public static void main(String[] args) {
        String[] user={"alex.lin@hudongpai.com"};
        EmailUtil.sendMail(user, "测试邮件test", "测试邮件test");
    }
}
