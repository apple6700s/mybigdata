package test;

import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.datatub.scavenger.extractor.KeywordsExtractor;
import com.yeezhao.commons.util.Entity.Params;
import org.junit.Test;

import java.util.List;

/**
 * test.TestScavenger
 *
 * @author lhfcws
 * @since 2017/6/9
 */
public class TestScavenger {
    @Test
    public void testKeyword() {
        KeywordsExtractor keywordsExtractor = KeywordsExtractor.getInstance();
        List<String> kws = keywordsExtractor.extract("[科技]速度对比！小米6 大战 三星S8！贵不一定好 这种对比手段特别傻逼～真的。一点意义也没有～");
        System.out.println(kws);
    }

    @Test
    public void testKeyword2() {
        Params p = new Params("content", "[科技]速度对比！小米6 大战 三星S8！贵不一定好 这种对比手段特别傻逼～真的。\n" +
                "一点意义也没有～\n" +
                "另外，好好练习你的普通发～");
        p = CommonLongTextAnalyzer.getInstance().analyz(p);
        System.out.println(p);
    }
}
