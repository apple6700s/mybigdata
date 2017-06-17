package com.datastory.banyan.wechat.tools;

import com.datastory.banyan.analyz.VerifyStatus;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.yeezhao.commons.util.AdvFile;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * com.datastory.banyan.wechat.tools.WechatMPMigrator
 * 先用 src/main/python/parse_wechat_mp_sqldump.py 从 db_yeezhao_stats.t_wechat_accounts 的 sql dump 中解析出 tsv，
 * 再用该类去从tsv导入hbase。
 *
 * 反正才一百多万个号，随便搞个线性单机了，devrhino 三台机差不多跑10-20min吧。
 *
 *
 * @author lhfcws
 * @since 16/12/13
 */

public class WechatMPMigrator implements Serializable {
    static String inputFile = "~/tmp/wechat_mp.tsv";
    static byte[] R = "r".getBytes();

    public void migrate() throws Exception {
        FileInputStream fis = new FileInputStream(inputFile);
        List<String> inputs = new ArrayList<>(AdvFile.readLines(fis));
        fis.close();
        String[] header = inputs.get(0).split("\t");
        System.out.println(inputs.get(0));

        final RFieldPutter putter = new RFieldPutter("DS_BANYAN_WECHAT_MP");

        final CountDownLatch latch = new CountDownLatch(inputs.size() - 1); // trim header
        ThreadPoolWrapper pool = ThreadPoolWrapper.getInstance(100);
        for (int i = 1; i < inputs.size(); i++) {
            final int index = i;
            final String[] row = inputs.get(i).split("\t");
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < row.length; j++) {
                            row[j] = row[j].trim();
                            if ("NULL".equals(row[j]))
                                row[j] = null;
                            else if (row[j].startsWith("'") && row[j].endsWith("'")) {
                                row[j] = row[j].replaceAll("^'", "").replaceAll("'$", "");
                            }
                        }

                        if (StringUtils.isEmpty(row[0]) && StringUtils.isNotEmpty(row[6]))
                            row[0] = row[6];

                        VerifyStatus vs = VerifyStatus.fromValueStr(row[4]);
                        if (vs != null)
                            row[4] = vs.getValue() + "";
                        else
                            row[4] = VerifyStatus.UNVERIFY.getValue() + "";

                        String pk = BanyanTypeUtil.sub3PK(row[3]);
                        Put put = new Put(pk.getBytes());
                        safeput(put, "wxid", row[0]);
                        safeput(put, "name", row[1]);
                        safeput(put, "open_id", row[2]);
                        safeput(put, "biz", row[3]);
                        safeput(put, "verify_status", row[4]);
                        safeput(put, "desc", row[5]);
                        safeput(put, "fans_cnt", row[6]);
                        safeput(put, "update_date", DateUtils.getCurrentTimeStr());
                        putter.batchWrite(put);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (index % 10000 == 0)
                            System.out.println("Progress: " + index);
                        latch.countDown();
                    }
                }
            });
        }
        System.out.println("Awaiting latch..., size : " + inputs.size());
        latch.await();
        putter.flush();
    }

    public static void safeput(Put put, String qualifier, String v) {
        if (v == null)
            return;
        put.addColumn(R, qualifier.getBytes(), v.getBytes());
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        if (args.length > 0)
            inputFile = args[0];

        new WechatMPMigrator().migrate();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
