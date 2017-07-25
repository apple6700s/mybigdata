package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.weibo.analyz.util.Util;
import com.datastory.banyan.weibo.doc.scavenger.ExtEntityDocMapper;
import com.datatub.scavenger.base.KbConsts;
import com.datatub.scavenger.extractor.EntityExtractor;
import com.datatub.scavenger.model.ExtEntity;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.component.LabelComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/16
 */
public class LabelComponentAnalyzer implements ComponentAnalyzer {

    public transient ExtEntity resEntity;

    @Override
    public List<String> getInputFields() {
        return Arrays.asList("content");
    }

    @Override
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter) {
        setup();
        for (Object o : valueIter) {
            streamingAnalyz(o);
        }
        return cleanup();
    }

    @Override
    public void setup() {
        resEntity = new ExtEntity();
    }

    @Override
    public void streamingAnalyz(Object o) {
        Map<String, String> mp = Util.transform(o);
        String content = mp.get("content");
        if (StringUtil.isNullOrEmpty(content))
            return;

        ExtEntity extEntity = EntityExtractor.extract(content);
        // emoji 暂不加入这边去分析，有专门的EmojiComponentAnalyzer
        extEntity.getEntitys().remove(KbConsts.EXTENTITY.EMOJI);
        resEntity.merge(extEntity);
    }

    @Override
    public StrParams cleanup() {
        StrParams ret = new StrParams();
        Params p = new ExtEntityDocMapper(resEntity).map();
        for (Map.Entry<String, Object> e : p.entrySet()) {
            if (e.getValue() instanceof String) {
                ret.put(e.getKey(), (String) e.getValue());
            }
        }
        return ret;
    }
}
