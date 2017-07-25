import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.RFieldPutter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.util.Date;
import java.util.Random;

/**
 * PACKAGE_NAME.TestHBase
 *
 * @author lhfcws
 * @since 2017/6/19
 */
public class TestHBase {
    @Test
    public void testVersionsDirectWrite() throws Exception {
        RFieldPutter writer = new RFieldPutter("VERSIONS_TEST");

        for (int i = 0; i < 5; i++)
            writer.batchWrite(makeVersionPut());

        writer.flush(true);


        Connection conn = ConnectionFactory.createConnection(RhinoETLConfig.getInstance());
        Table t = conn.getTable(TableName.valueOf("VERSIONS_TEST"));
        for (int i = 0; i < 3; i++) {
            Get get = new Get(String.valueOf(i).getBytes());
            get.setMaxVersions(30);
            Result r = t.get(get);
            System.out.println(r);
        }
    }

    private Put makeVersionPut() throws InterruptedException {
        long now = System.currentTimeMillis();
        String pk = new Random().nextInt(3) + "";
        Put put = new Put(pk.getBytes());
        put.addColumn("m".getBytes(), "taskId".getBytes(), new Date().toString().getBytes());
        put.addColumn("r".getBytes(), "name".getBytes(), ("lhfcws-" + pk).getBytes());
        System.out.println(put);
        Thread.sleep(900);
        return put;
    }
}
