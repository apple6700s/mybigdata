package com.datastory.commons3.es.lucene_writer;

import com.datastory.commons3.es.lucene_writer.directory.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;

/**
 * com.datastory.commons3.es.lucene_writer.HdfsIndexWriter
 *
 *
 *
 * @author lhfcws
 * @since 2017/5/4
 */
public class HdfsIndexWriter extends LocalIndexWriter {
    HdfsIndexWriter() {
        super();
    }

    public Configuration getConf() {
        HdfsDirectory directory = (HdfsDirectory) getDirectory();
        return directory.getConf();
    }
}
