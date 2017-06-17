package com.datastory.banyan.wechat.test;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.wechat.analyz.WechatEssayAnalyzer;
import com.datastory.banyan.wechat.doc.RhinoWechatContentDocMapper;
import com.datastory.banyan.wechat.doc.RhinoWechatMPDocMapper;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ILineParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * com.datastory.banyan.wechat.test.TestRhinoWechatConsumer
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class TestRhinoWechatConsumer {

    public void run(String fn) throws IOException {
        AdvFile.loadFileInRawLines(new FileInputStream(fn), new ILineParser() {
            @Override
            public void parseLine(String s) {
                process(s.trim());
            }
        });
    }

    public void process(String json) {
        WechatEssayAnalyzer analyzer = WechatEssayAnalyzer.getInstance(false);
        JSONObject jsonObject = JSONObject.parseObject(json);

        Params wechat = new RhinoWechatContentDocMapper(jsonObject).map();
        if (wechat == null || wechat.isEmpty()) {
            return;
//                            } else if (wechat.getString("view_cnt").equals("0")) {
//                                synchronized (compensateList) {
//                                    compensateList.add(wechat);
//                                }
        } else {
            wechat = analyzer.analyz(wechat);
            System.out.println("[WECHAT] " + wechat);
        }

        Params mp = new RhinoWechatMPDocMapper(jsonObject).map();
        if (mp != null) {
            System.out.println("[MP] " + mp);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        new TestRhinoWechatConsumer().run(args[0]);
        System.out.println("[PROGRAM] Program exited.");
    }
}
