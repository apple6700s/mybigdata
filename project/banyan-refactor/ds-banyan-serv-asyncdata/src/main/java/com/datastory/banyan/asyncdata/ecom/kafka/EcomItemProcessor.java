package com.datastory.banyan.asyncdata.ecom.kafka;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.asyncdata.ecom.doc.RhinoEcomItemDocMapper;
import com.datastory.banyan.asyncdata.ecom.hbase.EcomItemHbaseWriter;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.Entity.Params;

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
//        writer = EcomItemHbaseWriter.getInstance();
    }

    @Override
    public void _process(Object _p) {
        JSONObject jsonObject = (JSONObject) _p;
        Params hbParams = new RhinoEcomItemDocMapper(jsonObject.getJSONObject("info")).map();
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
        String json = "{\"type\":1,\"depth\":3,\"srcInfo\":{\"FILTER_TYPE\":\"1\",\"keyword\":\"卡夫 色拉酱\",\"TOP_N\":\"20\",\"crawler\":\"12\",\"start_date\":\"20170605120000\",\"end_date\":\"20170605170000\",\"lang\":\"\",\"time_zone\":\"PRC\"},\"jobName\":\"temp_ecomm_20170605183945_706_56\",\"cids\":[15],\"srcId\":14,\"info\":{\"taskId\":\"102155\",\"site\":\"麦乐购\",\"promo_price\":\"26\",\"review_count\":\"5\",\"item_image_url\":\"https://file.m6go.com/0uXLBBx0RkIBUVmHD~3PvA/50\",\"site_id\":\"-1\",\"promotion_info\":\"[{\\\"满额赠\\\":\\\"登录 满199.00元即赠热销商品，购物车领取赠品\\\"}]\",\"lang\":\"\",\"item_url\":\"http://www.gou.com/product_13961.html\",\"cat_id\":\"1\",\"unique_id\":\"c95d1c3e238e5c2ee0b25a43a3f78fdb\",\"sell_count\":\"186\",\"time_zone\":\"PRC\",\"title\":\"贝克曼博士Dr．Beckmann衣物除垢去污 酱渍与咖喱污渍克星50mL\",\"_html_\":\"避免过长，省略...\",\"price\":\"￥ 34\",\"item_id\":\"13961\",\"activity_info\":\"\",\"update_date\":\"20170605184052\",\"sell_count_total\":\"186\"},\"cacheObject\":{}}";
        new EcomItemProcessor(null)._process(JSONObject.parse(json));
    }
}
