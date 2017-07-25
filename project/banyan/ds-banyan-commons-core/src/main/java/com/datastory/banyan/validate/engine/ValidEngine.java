package com.datastory.banyan.validate.engine;

import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.rule.RuleSet;
import com.yeezhao.commons.util.Entity.Params;

/**
 * Created by abel.chan on 17/7/5.
 */
public interface ValidEngine {

    /**
     * 对指定上下文执行指定类型的规则
     *
     * @param context
     * @param ruleSetName
     */
    ValidResult execute(Params context, String ruleSetName) throws Exception;


    /**
     * 对指定上下文执行指定类型的规则
     *
     * @param context
     */
    ValidResult execute(Params context) throws Exception;

    /**
     * 添加一组规则
     *
     * @param ruleSet
     */
    void addRules(RuleSet ruleSet);

    /**
     * 删除一组规则
     *
     * @param ruleSet
     */
    void removeRules(RuleSet ruleSet);


}
