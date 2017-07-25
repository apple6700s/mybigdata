package com.datastory.commons3.es.lucene_writer;

import org.apache.lucene.codecs.CodecUtil;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogWriter;

import java.io.*;
import java.nio.file.Path;

/**
 * com.datastory.commons3.es.lucene_writer.TranslogIniter
 *
 * @author lhfcws
 * @since 2017/6/6
 */
public class TranslogIniter {
    public static final int MAGIC_CODE = CodecUtil.CODEC_MAGIC;
    public static final String TRANSLOG = "translog";
    public static final String TRANSLOG_UUID = Translog.TRANSLOG_UUID_KEY;
    public static final String TRANSLOG_GENERATION = Translog.TRANSLOG_GENERATION_KEY;
    public static final int VERSION = TranslogWriter.VERSION;


    public TranslogIniter() {
    }

    public String getPath(String pathIndexData) {
        File file = new File(pathIndexData);
        return file.getParent() + "/translog/translog-1.tlog";
    }

    public String getPath(String pathIndexData, long generation) {
        File file = new File(pathIndexData);
        return file.getParent() + "/translog/translog-" + generation + ".tlog";
    }

    public String getCkpPath(String pathIndexData) {
        File file = new File(pathIndexData);
        return file.getParent() + "/translog/" + Translog.CHECKPOINT_FILE_NAME;
    }

    public void writeInitCheckpoint(OutputStream os, int tlogSize) throws IOException {
        writeInitCheckpoint(os, tlogSize, 1);
    }

    /**
     * @param os
     * @param tlogSize
     * @param generation
     * @throws IOException
     * @see org.elasticsearch.index.translog.Checkpoint#write(org.apache.lucene.store.DataOutput)
     */
    public void writeInitCheckpoint(OutputStream os, int tlogSize, long generation) throws IOException {
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeLong(tlogSize);
        int numOps = 0;
        dos.writeInt(numOps);
        dos.writeLong(generation);

        dos.flush();
        dos.close();
    }

    /**
     * @param os
     * @param uuid
     * @return
     * @throws IOException
     * @see org.elasticsearch.index.translog.TranslogWriter#create(TranslogWriter.Type, ShardId, String, long, Path, Callback, int, TranslogWriter.ChannelFactory)
     */
    public int write(OutputStream os, String uuid) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(MAGIC_CODE);
        byte unknown = 8;
        dos.write(unknown);
        dos.write(TRANSLOG.getBytes());
        dos.writeInt(VERSION);
        dos.writeInt(uuid.getBytes().length);
        dos.write(uuid.getBytes());

        dos.flush();
        byte[] bs = baos.toByteArray();
        dos.close();

        os.write(bs);
        os.flush();
        os.close();

        return bs.length;
    }

    /**
     * 暂时不随机，做到可恢复
     *
     * @param index
     * @param hostname
     * @param shardId
     * @return
     * @see org.elasticsearch.index.translog.Translog#Translog(TranslogConfig)
     */
    public static String genUuid(String index, String hostname, int shardId) {
        String info = index + hostname + shardId;
        String fullId = Base64.encodeBytes(info.getBytes());
        return fullId;
    }

    public static String genUuid(EsShard shard) {
        return genUuid(shard.getIndex(), shard.getHost(), shard.getShardId());
    }
}
