package com.datastory.commons3.es.utils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * com.datastory.commons3.es.utils.SendEmails
 *
 * @author zhaozhen
 * @since 2017/6/22
 */
public class SendEmails {
    /* 配置信息*/ String SMTP_MAIL_HOST = "smtp.163.com"; // 此邮件服务器地址
    String EMAIL_USERNAME = "zhaozhen@hudongpai.com";
    String EMAIL_PASSWORD = "zhaozhen628";
    String TO_EMAIL_ADDRESS = "zhaozhen@hudongpai.com";

    public void sendMessage() throws Exception {
        /* 服务器信息 */
        final Properties props = new Properties();
        props.put("mail.smtp.host", SMTP_MAIL_HOST);
        props.put("mail.smtp.auth", "true");
        props.put("mail.user", EMAIL_USERNAME);
        props.put("mail.password", EMAIL_PASSWORD);

        Authenticator authenticator = new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                String userName = props.getProperty("mail.user");
                String password = props.getProperty("mail.password");
                return new PasswordAuthentication(userName, password);
            }
        };

        /* 创建Session */
        Session session = Session.getDefaultInstance(props, authenticator);

        /* 邮件信息 */
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(EMAIL_USERNAME));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(TO_EMAIL_ADDRESS));
        message.setSubject("邮件主题");
        message.setText("邮件内容");

        // 发送
        Transport.send(message);
    }
}
