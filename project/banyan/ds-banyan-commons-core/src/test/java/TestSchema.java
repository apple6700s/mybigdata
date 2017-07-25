import com.datastory.banyan.schema.PhWeiboSchema;
import com.datastory.banyan.schema.PhWeiboUserSchema;
import com.datastory.banyan.schema.Schema;
import com.datastory.banyan.schema.SchemaUtil;
import org.junit.Test;

/**
 * PACKAGE_NAME.TestSchema
 *
 * @author lhfcws
 * @since 2017/6/5
 */
public class TestSchema {
    @Test
    public void testSchemaUtil() {
        Schema schema = SchemaUtil.fromSchema(new PhWeiboSchema(), new PhWeiboUserSchema());
        System.out.println(schema.size());
    }
}
