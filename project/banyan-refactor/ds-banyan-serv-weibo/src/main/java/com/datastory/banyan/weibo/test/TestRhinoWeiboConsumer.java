package com.datastory.banyan.weibo.test;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.weibo.analyz.WbContentAnalyzer;
import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.datastory.banyan.weibo.doc.Status2HbParamsDocMapper;
import com.datastory.banyan.weibo.doc.User2HbParamsDocMapper;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.commons.lang3.StringUtils;
import weibo4j.model.Status;
import weibo4j.model.User;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * com.datastory.banyan.weibo.test.TestRhinoWeiboConsumer
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class TestRhinoWeiboConsumer {

    public void run(String fn) throws IOException {
        AdvFile.loadFileInRawLines(new FileInputStream(fn), new ILineParser() {
            @Override
            public void parseLine(String s) {
                process(s.trim());
            }
        });
    }

    public void process(String json) {
        JSONObject jsonObject = JSONObject.parseObject(json);

        final WbContentAnalyzer cntAnalyzer = WbContentAnalyzer.getInstance();
        final WbUserAnalyzer userAnalyzer = WbUserAnalyzer.getInstance();

        final Status status = GsonSerializer.deserialize(jsonObject.getString("json"), Status.class);
        if (status.getUser() == null || StringUtils.isEmpty(status.getUser().getId())) {
            return;
        }
        String updateDate = jsonObject.getString("update_date");

        // split status & user
        User statusUser = status.getUser();
        // 当前微博系统显示转发的那一条就是源微博，用//等可以分隔出大致的转发关系，也就是说转发链被压缩到用户发的微博中。
        Status srcStatus = status.getRetweetedStatus();
        User srcStatusUser = srcStatus == null ? null : srcStatus.getUser();

        // doc mapping status & user
        Params weibo = new Status2HbParamsDocMapper(status).map();
        Params srcWeibo = new Status2HbParamsDocMapper(srcStatus).map();
        Params user = new User2HbParamsDocMapper(statusUser).map();
        Params srcUser = new User2HbParamsDocMapper(srcStatusUser).map();

        if (weibo != null && user != null) {
            user.put("last_tweet_date", weibo.getString("publish_date"));
            if (user.get("uid") != null)
                weibo.put("uid", user.getString("uid"));
        }

        if (srcWeibo != null && srcUser != null) {
            srcUser.put("last_tweet_date", srcWeibo.getString("publish_date"));
            if (user.get("uid") != null)
                srcWeibo.put("uid", srcUser.getString("uid"));
            weibo.put("src_content", srcWeibo.getString("content"));
        }

        // analyz
        weibo = cntAnalyzer.analyz(weibo);
        srcWeibo = cntAnalyzer.analyz(srcWeibo);
        user = userAnalyzer.analyz(user);
        srcUser = userAnalyzer.analyz(srcUser);

        printIfNotNull(weibo);
        printIfNotNull(srcWeibo);
        printIfNotNull(user);
        printIfNotNull(srcUser);
    }

    public static void printIfNotNull(Params p) {
        if (p != null && !p.isEmpty()) {
            if (p.containsKey("mid")) {
                System.out.println("[WEIBO] " + p);
            } else
                System.out.println("[USER] " + p);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        new TestRhinoWeiboConsumer().run(args[0]);
        System.out.println("[PROGRAM] Program exited.");
    }
}
