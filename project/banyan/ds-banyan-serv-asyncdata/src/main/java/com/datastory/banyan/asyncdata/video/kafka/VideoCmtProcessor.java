package com.datastory.banyan.asyncdata.video.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdCommentDocMapper;
import com.datastory.banyan.asyncdata.video.doc.RhinoVideoCmtDocMapper;
import com.datastory.banyan.asyncdata.video.hbase.VdCommentPhoenixWriter;
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
 * com.datastory.banyan.asyncdata.video.kafka.VideoPostProcessor
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public class VideoCmtProcessor extends CountUpLatchBlockProcessor {
    private PhoenixWriter writer;

    public VideoCmtProcessor(CountUpLatch latch) {
        super(latch);
        writer = VdCommentPhoenixWriter.getInstance();
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            Params hbParams = new RhinoVideoCmtDocMapper(jsonObject).map();
            hbParams = CommonLongTextAnalyzer.getInstance().analyz(hbParams);

            writer.batchWrite(hbParams);
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
            String json = "{\"type\":1,\"depth\":4,\"srcInfo\":{\"sort\":\"sort_date\",\"FILTER_TYPE\":\"1\",\"keyword\":\"小米\",\"crawler\":\"115\",\"start_date\":\"20170606000000\",\"end_date\":\"20170616235959\",\"limit_date\":\"3\",\"lang\":\"\",\"time_zone\":\"PRC\"},\"jobName\":\"temp_video_20170608163021_079_5\",\"srcId\":118,\"info\":{\"taskId\":\"102153\",\"site\":\"优酷\",\"publish_date\":\"20170606000812\",\"site_id\":\"32\",\"lang\":\"\",\"url\":\"http://v.youku.com/v_show/id_XMjgwNjc2MTE5Ng\\u003d\\u003d.html\",\"cat_id\":\"4\",\"unique_id\":\"86361ab8f024ac2db6083cb0df058acd\",\"unique_parent_id\":\"ed8830ae51bf8899a5ca60e024ec0d23\",\"content\":\"这种对比手段特别傻逼～真的。\\n一点意义也没有～\\n另外，好好练习你的普通发～\",\"time_zone\":\"PRC\",\"author\":\"卡里五块八\",\"title\":\"[科技]速度对比！小米6 大战 三星S8！贵不一定好\",\"_html_\":\"避免过长，省略...\",\"_track_count_\":\"true\",\"item_id\":\"8c867ebf2940f5f599b6ecc3fe939ccd\",\"update_date\":\"20170608163132\",\"is_main_post\":\"0\",\"parent_id\":\"1b9fed4a2138ca45a0b68cacc0069150\"},\"cacheObject\":{}}";
            JSONObject jsonObject = JSON.parseObject(json);
            Params hbParams = new RhinoVideoCmtDocMapper(jsonObject).map();
            hbParams = CommonLongTextAnalyzer.getInstance().analyz(hbParams);
            System.out.println("====== size: " + hbParams.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(hbParams));
            Params esDoc = new Hb2EsVdCommentDocMapper(hbParams).map();

            System.out.println("====== size: " + esDoc.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(esDoc));
        } else if (args.length > 0) {
            String fn = args[0];
            final CountUpLatchBlockProcessor processor = new VideoCmtProcessor(null);
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
