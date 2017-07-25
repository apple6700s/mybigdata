package com.datastory.banyan.migrate1.flush;

import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomCommentDocMapper;
import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomItemDocMapper;
import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdCommentDocMapper;
import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdPostDocMapper;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.migrate1.mapreduce.TableSnapshotInputFormat;
import com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.weibo.doc.WbCntHb2ESDocMapper;
import com.datastory.banyan.weibo.doc.WbUserHb2ESDocMapper;
import com.datastory.commons3.es.lucene_writer.EsRouting;
import com.datastory.commons3.es.lucene_writer.ShardLocation;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import com.yeezhao.commons.util.serialize.ProtostuffSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.datastory.banyan.base.RhinoETLConsts.M;
import static com.datastory.banyan.migrate1.flush.FlushMR.*;

/**
 * com.datastory.banyan.migrate1.flush.RoutingMapper
 *
 * @author lhfcws
 * @since 2017/6/16
 */
public class RoutingMapper extends TableMapper<IntWritable, BytesWritable> {

    public static Logger LOG = Logger.getLogger(RoutingMapper.class);

    String host;
    String index;

    int totalShards;
    FlushParams flushParams = null;
    HashMap<String, ParamsDocMapper> docMappers = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        host = conf.get(PREFIX + "host");
        index = conf.get(PREFIX + "index");
        String fpJson = conf.get(PREFIX + "flushParams");
        this.flushParams = GsonSerializer.deserialize(fpJson, FlushParams.class);

        // 初始化 shard 的相关信息
        String slJson = conf.get(PREFIX + "shardLocation");
        ShardLocation shardLocation = GsonSerializer.deserialize(slJson, ShardLocation.class);
        if (shardLocation.getShardNum() == 0)
            throw new InterruptedException("Cannot get shard informations");

        totalShards = shardLocation.getShardNum();

        // init docMappers
        for (FlushParam fp : flushParams.values()) {
            String hbTable = fp.getOriTable();
            ParamsDocMapper dm = makeDocMapper(hbTable);
            docMappers.put(hbTable, dm);
        }
        LOG.info("[DocMappers] " + docMappers);
    }

    @Override
    /**
     * scan hbase，按 shardId 分发，ES 2.x使用Murmur3Hash做路由
     */
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        String hbTable = null;
        if (context.getInputSplit() instanceof TableSplit) {
            TableSplit tableSplit = (TableSplit) context.getInputSplit();
            hbTable = Bytes.toString(tableSplit.getTable().getName());
        } else if (context.getInputSplit() instanceof TableSnapshotInputFormat.TableSnapshotRegionSplit) {
            TableSnapshotInputFormat.TableSnapshotRegionSplit tableSplit = (TableSnapshotInputFormat.TableSnapshotRegionSplit) context.getInputSplit();
            hbTable = tableSplit.getOriTableName();
            LOG.info("[TABLE] " + hbTable);
        }
        assert hbTable != null;
        FlushParam fp = flushParams.get(hbTable);
        assert fp != null;

        context.getCounter(ScanFlushESMR.ROW.READ).increment(1);

        // transform doc
        Params hbDoc = result2Params(result, hbTable);
        Params esDoc = null;
        if (BanyanTypeUtil.valid(hbDoc)) {
            ParamsDocMapper docMapper = docMappers.get(hbTable);
            docMapper.setIn(hbDoc);
            esDoc = (Params) docMapper.map();
            if (esDoc != null) {
                cleanFields(esDoc, hbTable);
                esDoc.put("_type", fp.getType());   // 告诉reduce应该写入哪个type
            } else {
                LOG.error("[ERROR] [result] " + result);
                LOG.error("[ERROR] [hbDoc] " + hbDoc);
                context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
                return;
            }
        } else {
            LOG.error("[ERROR] [result] " + result);
            context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
            return;
        }

        if (!BanyanTypeUtil.valid(esDoc) || !esDoc.containsKey("id")) {
            if (!esDoc.containsKey("id"))
                LOG.error("[ERROR] [esDoc] " + esDoc);
            context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
            return;
        }

        try {
            // serialize
            Params.ParamsPojo pojo = new Params.ParamsPojo(esDoc);
            byte[] transfer = ProtostuffSerializer.serializeObject(pojo);

            // routing: 有parent用parent作routing，没有用id。原理参见ES 2.x的路由原理
            Integer shardId = EsRouting.routing(esDoc, totalShards);

            // write out
            context.write(new IntWritable(shardId), new BytesWritable(transfer));
            context.getCounter(ScanFlushESMR.ROW.MAP).increment(1);
        } catch (Exception e) {
            LOG.error("[ERROR] " + e.getMessage() + " : " + esDoc);
            context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
        }
    }

    /**
     * Result -> Params
     *
     * @param result
     * @return
     */
    private Params result2Params(Result result, String hbTable) {
        boolean isWbUserTable = hbTable.startsWith("DS_BANYAN_WEIBO_USER");
        Params p = new Params();
        p.put("pk", Bytes.toString(result.getRow()));
        Map<byte[], byte[]> rmap = result.getFamilyMap(R);
        if (rmap != null) {
            for (Map.Entry<byte[], byte[]> e : rmap.entrySet()) {
                p.put(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
            }
        }

        Map<byte[], byte[]> mmap = result.getFamilyMap(M);
        if (mmap != null) {
            for (Map.Entry<byte[], byte[]> e : mmap.entrySet()) {
                p.put(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
            }
        }

        Map<byte[], byte[]> fmap = result.getFamilyMap(F);
        if (isWbUserTable && fmap != null && !fmap.isEmpty()) {
            List<String> users = new LinkedList<>();
            for (byte[] uid : fmap.keySet()) {
                users.add(Bytes.toString(uid));
            }
            p.put("follow_list", users);
        }

        return p;
    }

    private ParamsDocMapper makeDocMapper(String hbTable) {
        ParamsDocMapper docMapper = null;
        // 根据HBase table 选择字段转换器
        if (hbTable.startsWith(Tables.table(Tables.PH_WBCNT_TBL))) {
            docMapper = new WbCntHb2ESDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_WBUSER_TBL))) {
            docMapper = new WbUserHb2ESDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_LONGTEXT_POST_TBL))) {
            docMapper = new NFPostHb2ESDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_LONGTEXT_CMT_TBL))) {
            docMapper = new NFCmtHb2ESDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_WXCNT_TBL))) {
            docMapper = new WxCntHb2ESDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_WXMP_TBL))) {
            docMapper = new WxMPHb2ESDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_ECOM_CMT_TBL))) {
            docMapper = new Hb2EsEcomCommentDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_ECOM_ITEM_TBL))) {
            docMapper = new Hb2EsEcomItemDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_VIDEO_POST_TBL))) {
            docMapper = new Hb2EsVdPostDocMapper(new Params());
        } else if (hbTable.startsWith(Tables.table(Tables.PH_VIDEO_CMT_TBL))) {
            docMapper = new Hb2EsVdCommentDocMapper(new Params());
        }

        if (docMapper == null) {
            LOG.error("[DOC] No docMapper implementation for " + hbTable);
        }
        return docMapper;
    }

    /**
     * 针对有问题的字段先做些特殊处理
     * @param esDoc
     * @param hbTable
     */
    private void cleanFields(Params esDoc, String hbTable) {
        if (hbTable.startsWith(Tables.table(Tables.PH_WXMP_TBL))) {
            esDoc.remove("fans_cnt");
        } else if (hbTable.startsWith(Tables.table(Tables.PH_WXCNT_TBL))) {
            if (BanyanTypeUtil.len(String.valueOf(esDoc.get("keywords"))) > 50) {
                esDoc.remove("keywords");
            }
        }
    }
}
