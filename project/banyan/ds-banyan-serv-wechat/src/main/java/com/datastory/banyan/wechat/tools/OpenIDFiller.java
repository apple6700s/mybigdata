package com.datastory.banyan.wechat.tools;

import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ILineParser;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.wechat.tools.OpenIDFiller
 *
 * @author lhfcws
 * @since 2017/3/13
 */
public class OpenIDFiller {
    public static void main(String[] args) throws Exception {
        String fn = "open_id.20170313.csv";
        final AtomicInteger cnt = new AtomicInteger(0);
        final RFieldPutter putter = new RFieldPutter("DS_BANYAN_WECHAT_MP");
        AdvFile.loadFileInRawLines(ClassUtil.getResourceAsInputStream(fn), new ILineParser() {
            @Override
            public void parseLine(String s) {
                String[] arr = s.split(",");
                String biz = arr[1];
                String pk = BanyanTypeUtil.sub3PK(biz);
                Params p = new Params();
                p.put("pk", pk);
                p.put("open_id", arr[2]);
                System.out.println(p);
                try {
                    putter.batchWrite(p);
                    cnt.addAndGet(1);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        putter.flush();

        System.out.println(cnt.get());
        System.out.println("[PROGRAM] Program exited.");
    }
}
