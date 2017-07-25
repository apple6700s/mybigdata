package com.datastory.banyan.validate.engine.impl;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.PropertiesUtils;
import com.datastory.banyan.validate.engine.ValidEngine;
import com.datastory.banyan.validate.entity.ExecuteResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.execute.Execute;
import com.datastory.banyan.validate.parser.Parser;
import com.datastory.banyan.validate.parser.impl.XmlParser;
import com.datastory.banyan.validate.rule.Rule;
import com.datastory.banyan.validate.rule.RuleSet;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by abel.chan on 17/7/5.
 */
public class XmlValidEngine implements ValidEngine {

    private static final String EXECUTE_LIST_PATH = "/execute-config.properties";
    private static final Logger LOG = Logger.getLogger(XmlValidEngine.class);

    private Map<String, RuleSet> ruleSetMap;
    private Map<String, Execute> ruleExecutes;

    private XmlValidEngine(String filePath) throws Exception {
        super();
        init(filePath, null);
    }

    private XmlValidEngine(InputStream inputStream) throws Exception {
        super();
        init(null, inputStream);
    }

    private static ConcurrentHashMap<String, ValidEngine> INSTANCE = new ConcurrentHashMap<>();

    public static ValidEngine getInstance(String filePath) throws Exception {
        String md5 = Md5Util.md5(filePath);
        if (!INSTANCE.containsKey(md5)) {
            synchronized (XmlValidEngine.class) {
                if (!INSTANCE.containsKey(md5)) {
                    INSTANCE.put(md5, new XmlValidEngine(filePath));
                }
            }
        }
        return INSTANCE.get(md5);
    }

    public static ValidEngine getInstance(InputStream inputStream) throws Exception {
        String md5 = Md5Util.md5(inputStream.toString());
        if (!INSTANCE.containsKey(md5)) {
            synchronized (XmlValidEngine.class) {
                if (!INSTANCE.containsKey(md5)) {
                    INSTANCE.put(md5, new XmlValidEngine(inputStream));
                }
            }
        }
        return INSTANCE.get(md5);
    }

    public void init(String filePath, InputStream inputStream) throws Exception {
        LOG.info("start init rule list !!!");
        Parser parser = new XmlParser();
        if (filePath == null && inputStream == null) {
            throw new NullPointerException("rule list init failed");
        }
        List<RuleSet> parse = filePath != null ? parser.parse(filePath) : parser.parse(inputStream);
        if (parse == null || parse.size() <= 0) {
            throw new Exception("rule list init failed !!!");
        }
        ruleSetMap = new ConcurrentHashMap<String, RuleSet>();
        for (RuleSet ruleSet : parse) {
            ruleSetMap.put(ruleSet.getName(), ruleSet);
        }

        LOG.info("rule list size: " + ruleSetMap.size() + ", rule set name list: " + ruleSetMap.keySet());
        LOG.info("finish init rule list !!!");

        LOG.info("start init execute list !!!");
        //加载所有的execute引擎
        Map<String, String> load = PropertiesUtils.load(EXECUTE_LIST_PATH);
        RhinoETLConfig config = RhinoETLConfig.getInstance();
        String validateExecuteList = config.get(RhinoETLConsts.VALIDATE_EXECUTE_LIST);

        ruleExecutes = new ConcurrentHashMap<String, Execute>();
        for (String execute : validateExecuteList.split(",")) {
            if (load.containsKey(execute)) {
                Class classType = Class.forName(load.get(execute));
                Execute obj = (Execute) classType.newInstance();
                ruleExecutes.put(execute, obj);
            }
        }
        LOG.info("execute size: " + ruleExecutes.size() + ", execute name list: " + ruleExecutes.keySet());
        LOG.info("finish init execute list !!!");
    }

    public ValidResult execute(Params paramse) throws Exception {
        if (ruleSetMap == null || ruleSetMap.size() <= 0) {
            throw new NullPointerException("ruleSet is null !!!!");
        }
        //获取map里面的第一个key
        return execute(paramse, ruleSetMap.keySet().iterator().next());
    }


    public ValidResult execute(Params params, String ruleSetName) throws Exception {
        List<Rule> rules = ruleSetMap.get(ruleSetName).getRules();
        if (rules != null) {
            return processRuleSet(params, rules);
        } else {
            throw new NullPointerException("ruleSet is null !!!");
        }
    }

    private ValidResult processRuleSet(Params params, List<Rule> rules) throws Exception {
        ValidResult validResult = new ValidResult();
        //如果没有后续规则，则退出
        if (rules.size() == 0) {
            //说明没有规则，返回通过即可。
            validResult.setSuccess(true);
            return validResult;
        }

        if (ruleExecutes == null || ruleExecutes.size() <= 0) {
            throw new NullPointerException("valid Execute can't be null !!!");
        }

        for (Rule rule : rules) {
            for (Map.Entry<String, Execute> entry : ruleExecutes.entrySet()) {
                ExecuteResult executeResult = entry.getValue().execute(params, rule);
                //将executeResult的结果写入到validResult变量。
                entry.getValue().writeResult(rule, executeResult, validResult);
            }
        }

        //检查validReuslt字段是否存在冲突，如一个字段均出现在null和success中，且iscannull等于false；若有，则从success移除
        for (Rule rule : rules) {
            if (!rule.isCanNull()) {
                //检查是否需要将将succeeField存在nullField中移除
                if (validResult.getNullFields().contains(rule.getField()) && validResult.getSuccessFields().contains(rule.getField())) {
                    validResult.getSuccessFields().remove(rule.getField());
                }
            }
        }

        if (!validResult.isErrorDelete() && !validResult.isErrorIgnore()) {
            validResult.setSuccess(true);
        }
        return validResult;
    }

    public void addRules(RuleSet newRuleSet) {
        if (!ruleSetMap.containsKey(newRuleSet.getName())) {
            ruleSetMap.put(newRuleSet.getName(), new RuleSet(newRuleSet.getName()));
        }

        RuleSet ruleSet = ruleSetMap.get(newRuleSet.getName());

        //检查规则
        for (Rule rule : newRuleSet.getRules()) {
            if (rule.isRuleValid()) {
                ruleSet.addRule(rule);
            } else {
                LOG.error(String.format("规则[%s]检查无效.", rule.toString()));
            }
        }
    }


    public void removeRules(RuleSet ruleSet) {
        List<Rule> rules = ruleSetMap.get(ruleSet.getName()).getRules();
        if (rules != null) {
            rules.removeAll(ruleSet.getRules());
        }
    }

}
