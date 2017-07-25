package com.datastory.banyan.asyncdata.ecom.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomItemDocMapper;
import com.datastory.banyan.asyncdata.ecom.doc.RhinoEcomItemDocMapper;
import com.datastory.banyan.asyncdata.ecom.hbase.EcomItemHbaseWriter;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ILineParser;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * com.datastory.banyan.asyncdata.ecom.kafka.EcomItemProcessor
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class EcomItemProcessor extends CountUpLatchBlockProcessor {
    private EcomItemHbaseWriter writer;

    public EcomItemProcessor(CountUpLatch latch) {
        super(latch);
        writer = EcomItemHbaseWriter.getInstance();
    }

    @Override
    public void _process(Object _p) {
        JSONObject jsonObject = (JSONObject) _p;
        Params hbParams = new RhinoEcomItemDocMapper(jsonObject).map();
        try {
            writer.batchWrite(hbParams);
        } catch (IOException e) {
            ErrorUtil.simpleError(e);
        }
    }

    @Override
    public void cleanup() {
        if (processSize.get() > 0) {
            try {
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            ESWriterAPI esWriterAPI = writer.getEsWriter();
            if (esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            String json = "{\"type\":0,\"depth\":3,\"srcInfo\":{\"FILTER_TYPE\":\"1\",\"keyword\":\"美味源 清酱汁\",\"TOP_N\":\"20\",\"crawler\":\"4443\",\"FILTER\":\"金唛|美味源快餐\",\"start_date\":\"20170605120000\",\"end_date\":\"20170605170000\",\"lang\":\"\",\"time_zone\":\"null\"},\"jobName\":\"temp_ecomm_20170608163348_900_77\",\"cids\":[4446],\"srcId\":4445,\"info\":{\"promo_price\":\"¥ 59.40\",\"item_image_url\":\"http://i3.chunboimg.com/group1/M00/00/98/Cv4JrVhBTK2AEgOTAAVBhn_g6F0215_200_200.jpg\",\"review_count\":\"20\",\"platform_score\":\"100\",\"lang\":\"\",\"unique_id\":\"bc39f5623adc507ee53bcfe0c7399e79\",\"cat_id\":\"1\",\"title\":\"台湾原装鱻采小卷200克\",\"_html_\":\"避免过长，省略...\",\"full_url\":\"http://www.chunbo.com/product/26041.html\",\"site\":\"春播\",\"taskId\":\"102155\",\"item_title\":\"台湾原装鱻采小卷200克\",\"site_id\":\"129469\",\"other_data\":\"{\\\"品牌 | 产地\\\":\\\"| 台湾\\\",\\\"储存方法\\\":\\\"冰箱冷冻保存\\\",\\\"保质期\\\":\\\"2年\\\",\\\"规格\\\":\\\"200g\\\"}\",\"promotion_info\":\"[{\\\"限时抢购\\\":\\\"限时抢购\\\"}]\",\"url\":\"http://www.chunbo.com/product/26041.html\",\"item_url\":\"http://www.chunbo.com/product/26041.html\",\"shop_name\":\"春播\",\"time_zone\":\"null\",\"price\":\"¥ 66.00\",\"repertory\":\"上海\",\"_track_count_\":\"true\",\"update_date\":\"20170608164138\"},\"cacheObject\":{}}";
            JSONObject jsonObject = JSON.parseObject(json);

            Params hbParams = new RhinoEcomItemDocMapper(jsonObject).map();
            System.out.println("====== size: " + hbParams.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(hbParams));
            Params esDoc = new Hb2EsEcomItemDocMapper(hbParams).map();

            System.out.println("====== size: " + esDoc.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(esDoc));
        } else if (args.length > 0) {
            String fn = args[0];
            final CountUpLatchBlockProcessor processor = new EcomItemProcessor(null);
            AdvFile.loadFileInRawLines(new FileInputStream(fn), new ILineParser() {
                @Override
                public void parseLine(String s) {
                    JSONObject jsonObject = JSON.parseObject(s);
                    try {
                        processor.process(jsonObject);
                    } catch (Exception e) {
                        System.err.println(s);
                        e.printStackTrace();
                    }
                }
            });
            processor.cleanup();
        }
        System.exit(0);
    }
}
