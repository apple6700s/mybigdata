package com.datastory.banyan.weibo.analyz.component;

import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.serialize.ProtostuffSerializer;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

/**
 * com.datastory.banyan.weibo.analyz.component.ComponentAnalyzer
 *
 * @author lhfcws
 * @since 2016/12/23
 */
public interface ComponentAnalyzer extends Serializable {
    public static Logger LOG = Logger.getLogger(ComponentAnalyzer.class);

    public List<String> getInputFields();
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter);
    public void setup();
    public void streamingAnalyz(Object obj);
    public StrParams cleanup();

}
