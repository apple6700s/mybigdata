package com.datastory.banyan.asyncdata.video.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdPostDocMapper;
import com.datastory.banyan.asyncdata.video.doc.RhinoVideoPostDocMapper;
import com.datastory.banyan.asyncdata.video.hbase.VdPostPhoenixWriter;
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
public class VideoPostProcessor extends CountUpLatchBlockProcessor {
    private PhoenixWriter writer;

    public VideoPostProcessor(CountUpLatch latch) {
        super(latch);
        writer = VdPostPhoenixWriter.getInstance();
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            Params hbParams = new RhinoVideoPostDocMapper(jsonObject).map();
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
            String json = "{\"type\":1,\"depth\":4,\"srcInfo\":{\"sort\":\"sort_date\",\"FILTER_TYPE\":\"1\",\"keyword\":\"小米\",\"crawler\":\"127\",\"start_date\":\"20170606000000\",\"end_date\":\"20170616235959\",\"pubtime\":\"3\",\"lang\":\"\",\"time_zone\":\"PRC\"},\"jobName\":\"temp_video_20170608163021_079_5\",\"cids\":[131],\"srcId\":130,\"info\":{\"taskId\":\"102153\",\"site\":\"腾讯视频\",\"publish_date\":\"20170513134544\",\"review_count\":\"0\",\"site_id\":\"35\",\"video_comment_id\":\"\",\"lang\":\"\",\"url\":\"https://v.qq.com/x/page/w0502rx6oqi.html\",\"content\":\"小米6厉害了\",\"cat_id\":\"4\",\"unique_id\":\"fd7020e28961e0afc06418af5ffe7959\",\"author\":\"航佳数码\",\"time_zone\":\"PRC\",\"title\":\"小米6厉害了\",\"thumbnail\":\"https://puui.qpic.cn/qqvideo_ori/0/w0502rx6oqi_228_128/0\",\"_html_\":\"避免过长，省略...\",\"_track_count_\":\"true\",\"item_id\":\"w0502rx6oqi\",\"update_date\":\"20170608164213\",\"video_id\":\"w0502rx6oqi\",\"is_main_post\":\"1\",\"full_url\":\"https://v.qq.com/x/page/w0502rx6oqi.html\",\"view_count\":\"1176\"},\"cacheObject\":{}}";
            JSONObject jsonObject = JSON.parseObject(json);
            Params hbParams = new RhinoVideoPostDocMapper(jsonObject).map();
            hbParams = CommonLongTextAnalyzer.getInstance().analyz(hbParams);
            System.out.println("====== size: " + hbParams.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(hbParams));
            Params esDoc = new Hb2EsVdPostDocMapper(hbParams).map();

            System.out.println("====== size: " + esDoc.size());
            System.out.println(BanyanTypeUtil.prettyStringifyMap(esDoc));
        } else if (args.length > 0) {
            String fn = args[0];
            final CountUpLatchBlockProcessor processor = new VideoPostProcessor(null);
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
