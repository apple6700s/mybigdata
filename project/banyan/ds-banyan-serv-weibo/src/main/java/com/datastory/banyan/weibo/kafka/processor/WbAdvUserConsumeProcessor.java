package com.datastory.banyan.weibo.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.hbase.Connections;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.weibo.analyz.AdvUserJsonProcessor;
import com.datastory.banyan.weibo.analyz.BirthYearExtractor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.weibo.kafka.processor.WbAdvUserConsumeProcessor
 *
 * @author lhfcws
 * @since 2017/4/27
 */
public class WbAdvUserConsumeProcessor extends CountUpLatchBlockProcessor {
    static final byte[] F_FAMILY = "f".getBytes();
    static final byte[] R_FAMILY = "r".getBytes();
    static final HashMap<String, String> MAPPING_FIELD = BanyanTypeUtil.strArr2strMap(new String[]{
            "career", "company",
            "birthday", "birthdate",
            "education", "school",
            "tags", "tags",
    });

    List<Put> userPuts = new LinkedList<>();
    List<Delete> dels = new LinkedList<>();
    BanyanRFieldPutter putter = new BanyanRFieldPutter(Tables.table(Tables.PH_WBUSER_TBL));

    public WbAdvUserConsumeProcessor(CountUpLatch latch) {
        super(latch);
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            String type = jsonObject.getString("type");
            String updateDate = jsonObject.getString("update_date");
            String json = jsonObject.getString("json");
            String uid = jsonObject.getString("uid");
            String pk = BanyanTypeUtil.wbuserPK(uid);

            // advUserField
            Put put = new Put(pk.getBytes());
            put.addColumn(R_FAMILY, "update_date".getBytes(), updateDate.getBytes());

            Object parseRes = AdvUserJsonProcessor.process(type, json);
            if ("bilateral_follow".equals(type)) {
                Delete delete = new Delete(pk.getBytes());
                delete.addFamily(F_FAMILY);
                synchronized (dels) {
                    dels.add(delete);
                }

                Map<String, String> map = (Map<String, String>) parseRes;
                if (map != null)
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        put.addColumn(F_FAMILY, entry.getKey().getBytes(), entry.getValue().getBytes());
                    }
            } else {
                String value = (String) parseRes;
                String field = MAPPING_FIELD.get(type);
                if (field == null)
                    return;

                if ("birthdate".equals(field)) {
                    String birthYear = BirthYearExtractor.extract(value);
                    if (birthYear == null)
                        return;
                    put.addColumn(R_FAMILY, "birthyear".getBytes(), birthYear.getBytes());
                }
                put.addColumn(R_FAMILY, field.getBytes(), value.getBytes());
            }

            synchronized (userPuts) {
                userPuts.add(put);
            }
            if (userPuts.size() > 100) {
                flushDels(Tables.table(Tables.PH_WBUSER_TBL));
                flush(false);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        if (getProcessedSize() > 0) {
            if (dels.size() > 0)
                flushDels(Tables.table(Tables.PH_WBUSER_TBL));

            if (userPuts.size() > 0)
                try {
                    flush(true);
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
        }
    }

    private void flushDels(String table) {
        if (dels.isEmpty()) return;

        List<Delete> deletes;
        synchronized (dels) {
            deletes = new ArrayList<>(dels);
            dels.clear();
        }

        Connection conn = null;
        Table hti = null;
        try {
            conn = Connections.get();
            hti = Connections.getTable(conn, table);

            hti.delete(deletes);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                Connections.close(conn, hti);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void flush(boolean awaitFlush) throws IOException {
        if (userPuts.isEmpty()) return;
        List<Put> list;

        synchronized (userPuts) {
            list = new ArrayList<>(userPuts);
            userPuts.clear();
        }

        try {
            for (Put put : list)
                putter.batchWrite(put);
            putter.flush(awaitFlush);
            list.clear();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
        }
    }
}
