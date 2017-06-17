package com.datastory.banyan.wechat.kafka.processor;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.hbase.hooks.HBasePutRetryLogHook;
import com.datastory.banyan.wechat.analyz.WechatEssayAnalyzer;
import com.datastory.banyan.wechat.es.WxCntESWriter;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.sql.execution.columnar.BOOLEAN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.wechat.kafka.processor.WxContentCompensator
 *
 * @author lhfcws
 * @since 2017/2/17
 */
public class WxContentCompensator {
    static HBaseReader hBaseReader = null;
    WechatEssayAnalyzer analyzer = null;
    BanyanRFieldPutter putter = null;

    static {
        hBaseReader = new HBaseReader() {
            @Override
            public String getTable() {
                return Tables.table(Tables.PH_WXCNT_TBL);
            }

            @Override
            /**
             * 不关心返回结果，只要不是null就好，节省解析开销
             */
            public Params result2Params(Result result) {
                return new Params();
            }
        };
    }

    public WxContentCompensator(BanyanRFieldPutter putter, WechatEssayAnalyzer analyzer) {
        this.analyzer = analyzer;
        this.putter = putter;
    }

    public void write(List<Params> plist) throws Exception {
        if (plist.isEmpty()) return;

        LinkedList<Get> gets = new LinkedList<>();
        for (Params p : plist) {
            String pk = p.getString("pk");
            Get get = hBaseReader.makeExistGet(pk);
            gets.add(get);
        }

        List<Params> toWrite = new LinkedList<>();
        try {
            List<Params> ret = hBaseReader.flush(gets);
            plist = new ArrayList<>(plist);

            int i = 0;
            for (Params p : ret) {
                if (p == null) {
                    toWrite.add(plist.get(i));
                }
                i++;
            }
            ret.clear();

        } catch (IOException e) {
            HBasePutRetryLogHook hook = new HBasePutRetryLogHook("hbase." + Tables.table(Tables.PH_WXCNT_TBL));
            hook.afterWrite(plist, true);
        }

        if (!toWrite.isEmpty())
            for (Params p : toWrite) {
                this.analyzer.analyz(p);
                this.putter.batchWrite(p);
            }
    }

    public void flush() throws IOException {
        this.putter.flush();
    }
}
