import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.junit.Test;

/**
 * PACKAGE_NAME.TestMurmur3
 *
 * @author lhfcws
 * @since 2017/2/7
 */
public class TestMurmur3 {
    static Murmur3HashFunction hashFunction = new Murmur3HashFunction();

    @Test
    public void testHash() {
        int m = 48;
        System.out.println(hashFunction.hash("1774089470") % m);
        System.out.println(hashFunction.hash("1774089471") % m);
        System.out.println(hashFunction.hash("1994626454") % m);
        System.out.println(hashFunction.hash("2619983742") % m);
    }
}
