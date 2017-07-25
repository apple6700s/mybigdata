package com.datastory.banyan.asyncdata.ecom.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomCommentDocMapper;
import com.datastory.banyan.asyncdata.ecom.doc.RhinoEcomCmtDocMapper;
import com.datastory.banyan.asyncdata.ecom.hbase.EcomCommentPhoenixWriter;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.CountUpLatch;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ILineParser;

import java.io.FileInputStream;

/**
 * com.datastory.banyan.asyncdata.ecom.kafka.EcomItemProcessor
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class EcomCmtProcessor extends CountUpLatchBlockProcessor {
    private PhoenixWriter writer;

    public EcomCmtProcessor(CountUpLatch latch) {
        super(latch);
        writer = EcomCommentPhoenixWriter.getInstance();
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            Params hbDoc = new RhinoEcomCmtDocMapper(jsonObject).map();
            hbDoc = CommonLongTextAnalyzer.getInstance().analyz(hbDoc);
            writer.batchWrite(hbDoc);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        if (processSize.get() > 0) {
            writer.flush();
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
            String json = "{\"type\":1,\"depth\":4,\"srcInfo\":{\"FILTER_TYPE\":\"1\",\"keyword\":\"味事达 生抽\",\"TOP_N\":\"20\",\"crawler\":\"16\",\"start_date\":\"20170605120000\",\"end_date\":\"20170605170000\",\"lang\":\"\",\"time_zone\":\"PRC\"},\"jobName\":\"temp_ecomm_20170608163348_900_77\",\"srcId\":610,\"info\":{\"comment_id\":\"ed160840287d4ba9802e52d6499bf84f\",\"comment_url\":\"http://club.jd.com/repay/3715290_9a8bcc4b-6ba0-4a15-9c3a-8af6a66f6363_1.html\",\"reference_name\":\"【京东超市】味事达 Master 金标生抽王 500ml\",\"image_url_list\":\"[]\",\"score\":\"4\",\"lang\":\"\",\"cat_id\":\"1\",\"unique_parent_id\":\"857e5d63bac31f4799b8b0fa8918a024\",\"unique_id\":\"440ba807708d156a812d03a779aedc16\",\"author\":\"l***u\",\"title\":\"【京东超市】味事达 Master 金标生抽王 500ml\",\"_html_\":\"避免过长，省略...\",\"level\":\"铜牌会员\",\"province\":\"\",\"parent_id\":\"3715290\",\"taskId\":\"102155\",\"site\":\"京东\",\"publish_date\":\"20170605151013\",\"self_support\":\"false\",\"item_title\":\"【京东超市】味事达 Master 金标生抽王 500ml\",\"site_id\":\"5\",\"url\":\"http://item.jd.com/3715290.html\",\"reference_date\":\"20170306234314\",\"shop_name\":\"\",\"content\":\"还不错，挺鲜的，就这样吧\",\"time_zone\":\"PRC\",\"comment_date\":\"20170605151013\",\"_track_count_\":\"true\",\"item_id\":\"3715290\",\"like_count\":\"0\",\"update_date\":\"20170608165603\",\"CONTROL_PAGE\":\"0\"},\"cacheObject\":{}}";
            JSONObject jsonObject = JSON.parseObject(json);

            Params hbDoc = new RhinoEcomCmtDocMapper(jsonObject).map();
            hbDoc = CommonLongTextAnalyzer.getInstance().analyz(hbDoc);
            System.out.println("====== size: " + hbDoc.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(hbDoc));
            Params esDoc = new Hb2EsEcomCommentDocMapper(hbDoc).map();

            System.out.println("====== size: " + esDoc.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(esDoc));
        } else if (args.length > 0) {
            String fn = args[0];
            final CountUpLatchBlockProcessor processor = new EcomCmtProcessor(null);
            AdvFile.loadFileInRawLines(new FileInputStream(fn), new ILineParser() {
                @Override
                public void parseLine(String s) {
                    JSONObject jsonObject = JSON.parseObject(s);
                    try {
                        processor.process(jsonObject);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            processor.cleanup();
        }
        System.exit(0);
    }
}
