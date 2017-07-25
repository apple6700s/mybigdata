package com.datastory.commons3.es.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import spock.lang.Specification
/**
 *
 * com.datastory.commons3.es.test.AnySpec
 * @author lhfcws
 * @since 2017/6/7
 */
class AnySpec extends Specification {
    def "seek pos"() {
        when:
        String CODEC = "Lucene50FieldInfos";
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml")
        conf.addResource("hdfs-site.xml")

        String hdfsFn = "/tmp/lhfcws/ds-es1/0/index/_0.cfs";
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream is = fs.open(new Path(hdfsFn));
        is.seek(5663);

        byte[] b = new byte[CODEC.length()];
        is.read(b);
        String actual = new String(b)

        then:
        actual == CODEC
    }
}
