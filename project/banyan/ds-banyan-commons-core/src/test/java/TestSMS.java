import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.SMSClient;
import org.junit.Test;

/**
 * PACKAGE_NAME.TestSMS
 *
 * @author lhfcws
 * @since 2017/2/8
 */
public class TestSMS {
    @Test
    public void testUnsendSMS() {
        SMSClient.DEBUG = true;
        testSendSMS();
    }

    @Test
    public void testSendSMS() {
        SMSClient.send(SMSClient.ES_ERROR_ALARM, DateUtils.getCurrentPrettyTimeStr(), "test-db", "server is down? 中文测试, dont panic.");
    }
}
