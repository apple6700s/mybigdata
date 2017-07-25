import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.Test;

/**
 * PACKAGE_NAME.TestEncode
 *
 * @author lhfcws
 * @since 2017/3/14
 */
public class TestEncode {
    @Test
    public void testUnescape() {
        String u = "\\u5C0F\\u6797\\u4E5F\\u4E0D\\u8BB0\\u5F97\\u5565\\u65F6\\u5019\\u7684\\u4E8B\\u60C5\\u4E86\\u3002";
        String s = StringEscapeUtils.unescapeJava(u);
        System.out.println(s);
        s = StringEscapeUtils.unescapeJava(s);
        System.out.println(s);
    }

    @Test
    public void testEmoji() {
        String u = "\uD83D\uDE01asdas好呀" + StringEscapeUtils.escapeJava("英文");
        System.out.println(u);
        u = removeNonBmpUnicode(u);
        System.out.println(u);
        u = StringEscapeUtils.unescapeJava(u);
        System.out.println(u);
        u = StringEscapeUtils.unescapeJava(u);
        System.out.println(u);
    }

    public static String removeNonBmpUnicode(String str) {
        if (str == null) {
            return null;
        }
        str = str.replaceAll("[^\\u0000-\\uFFFF]", "");
        return str;
    }
}
