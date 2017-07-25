package com.datastory.banyan.weibo.analyz.user_component;

import com.yeezhao.commons.util.Entity.StrParams;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.user_component.UserAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public interface UserAnalyzer extends Serializable {
    public List<String> getInputFields();
    public String getOutputTable();
    public StrParams analyz(Map<String, String> user);
}
