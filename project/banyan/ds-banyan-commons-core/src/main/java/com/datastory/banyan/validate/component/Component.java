package com.datastory.banyan.validate.component;

import com.datastory.banyan.validate.rule.ExecuteRule;
import com.datastory.banyan.validate.rule.Rule;

/**
 * Created by abel.chan on 17/7/6.
 */
public interface Component<T extends Rule, K extends ExecuteRule> {

    /**
     * 执行规则，并把结果放到上下文上
     *
     * @param field
     * @param value
     * @param rule
     * @param executeRule
     * @return
     */
    boolean execute(String field, Object value, T rule, K executeRule);
}
