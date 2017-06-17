package com.datastory.banyan.newsforum.test;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer;
import com.datastory.banyan.newsforum.doc.Rhino2NewsForumDocMapper;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ILineParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * com.datastory.banyan.newsforum.test.TestRhinoNewsForumConsumer
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class TestRhinoNewsForumConsumer {

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

        List<Params> list = new Rhino2NewsForumDocMapper(jsonObject).map();

//                            if (list != null && getFilters().size() > 0) {
//                                Iterator<Params> iter = list.iterator();
//                                while (iter.hasNext()) {
//                                    Params p = iter.next();
//                                    for (Filter filter : getFilters()) {
//                                        if (filter.isFiltered(p)) {
//                                            iter.remove();
//                                            stat.filterStat().inc();
//                                            break;
//                                        }
//                                    }
//                                }
//                            }

        if (CollectionUtil.isEmpty(list)) return;

        // all_content
        Params mainPost = list.get(0);
        if (mainPost.getString("is_main_post").equals("0"))
            mainPost = null;
        if (mainPost != null) {
            StringBuilder allContent = new StringBuilder(mainPost.getString("content"));
            for (int i = 1; i < list.size(); i++) {
                Params p = list.get(i);
                if (p.get("content") != null)
                    allContent.append(p.getString("content"));
            }
            mainPost.put("all_content", allContent.toString());
        }
        NewsForumAnalyzer analyzer = NewsForumAnalyzer.getInstance();
        for (int i = 0; i < list.size(); i++) {
            try {
                Params p = list.get(i);
                p = analyzer.analyz(p);

                list.set(i, p);

                if (analyzer.isMainPost(p))
                    System.out.println("[POST] " + p);
//                    postWriter.batchWrite(p);
                else
                    System.out.println("[COMMENT] " + p);
//                    cmtWriter.batchWrite(p);
            } catch (Throwable e) {
            } finally {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        new TestRhinoNewsForumConsumer().run(args[0]);
        System.out.println("[PROGRAM] Program exited.");
    }
}
