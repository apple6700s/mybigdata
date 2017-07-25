package com.datastory.commons3.es.copyData;

import com.datastory.commons3.es.lucene_writer.InitTranslog;
import com.datastory.commons3.es.utils.SendEmails;
import org.apache.hadoop.conf.Configuration;

/**
 * com.datastory.commons3.es.copyData.HttpWorkNode
 *
 * @author zhaozhen
 * @since 2017/6/21
 */
public class HttpWorkNode {

    public static void doWork(String esHost, int port, String cluster_name, String index, String hdfs_data_root, String dir) throws Exception {

        //copy hdfs data to local
        int totalShards = EsIndexOperation.totalShards(esHost, port, cluster_name, index);
        //close index
        EsIndexOperation.closeEsIndex(esHost, port, cluster_name, index);

        CopyHdfsData copyHdfsData = new CopyHdfsData();

        //copy hdfs data
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        copyHdfsData.doCopy(totalShards, dir, index, conf);

        //unzip
        copyHdfsData.unZipFiles(totalShards, dir, index);

        //translog init
        InitTranslog.createTranslog(esHost, port, cluster_name, index);

        //del hdfs data
        copyHdfsData.delHdfsData(hdfs_data_root);

        //open index
        EsIndexOperation.openEsIndex(esHost, port, cluster_name, index);

        //send message
        SendEmails sendEmails = new SendEmails();
        sendEmails.sendMessage();
    }

}
