package com.datastory.banyan.validate.parser.impl;

import com.datastory.banyan.validate.parser.Parser;
import com.datastory.banyan.validate.rule.ExecuteRule;
import com.datastory.banyan.validate.rule.Rule;
import com.datastory.banyan.validate.rule.RuleSet;
import com.google.common.collect.Maps;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.JXMLQuery;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Created by abel.chan on 17/7/5.
 */
public class XmlParser implements Parser {

    @Override
    public List<RuleSet> parse(String filePath) throws Exception {
        InputStream inputStream = new FileInputStream(new File(filePath));
        return parse(inputStream);
    }

    @Override
    public List<RuleSet> parse(InputStream inputStream) throws Exception {
        Map<String, RuleSet> ruleSetMap = Maps.newHashMap();
        JXMLQuery jxmlQuery = JXMLQuery.load(inputStream);
        JXMLQuery.JXMLQueryList children = jxmlQuery.children();
        Iterator<JXMLQuery> iterator = children.iterator();
        while (iterator.hasNext()) {
            RuleSet ruleSet = getRuleSet(iterator.next());
            if (ruleSetMap.containsKey(ruleSet.getName())) {
                ruleSetMap.get(ruleSet.getName()).addRule(ruleSet.getRules());
            } else {
                ruleSetMap.put(ruleSet.getName(), ruleSet);
            }
        }
        List<RuleSet> ruleSets = new ArrayList<>();
        ruleSets.addAll(ruleSetMap.values());
        return ruleSets;
    }

    @Deprecated
    public Rule getRule(JXMLQuery jxmlQuery) {
        Rule rule = new Rule();
        //获取基本属性
        rule.setCanNull(Boolean.parseBoolean(getAttrValue(jxmlQuery, "iscannull")));
        rule.setType(getAttrValue(jxmlQuery, "type"));
        rule.setRecordZero(Boolean.parseBoolean(getAttrValue(jxmlQuery, "isrecordzero")));
        rule.setErrorLevel(getAttrValue(jxmlQuery, "errorlevel"));
        rule.setField(getFieldValue(jxmlQuery, "fields"));
        rule.setDescription(getFieldValue(jxmlQuery, "description"));

        //获取executeRule属性
        List<ExecuteRule> executeRules = getExecuteRules(jxmlQuery);
        if(executeRules!=null) {
            rule.setExecuteRules(executeRules);
        }

        return rule;
    }

    public List<Rule> getRules(JXMLQuery jxmlQuery) {
        List<Rule> rules = new LinkedList<>();
        String[] fields = getFieldValue(jxmlQuery, "field").split(",");

        Rule ruleTemplate = new Rule();
        //获取基本属性
        ruleTemplate.setCanNull(Boolean.parseBoolean(getAttrValue(jxmlQuery, "iscannull")));
        ruleTemplate.setType(getAttrValue(jxmlQuery, "type"));
        ruleTemplate.setRecordZero(Boolean.parseBoolean(getAttrValue(jxmlQuery, "isrecordzero")));
        ruleTemplate.setErrorLevel(getAttrValue(jxmlQuery, "errorlevel"));
        ruleTemplate.setDescription(getFieldValue(jxmlQuery, "description"));

        //获取executeRule属性
        ruleTemplate.setExecuteRules(getExecuteRules(jxmlQuery));

        for (String field : fields) {
            field = field.trim();
            Rule rule = ruleTemplate.clone();
            rule.setField(field);
            rules.add(rule);
        }
        return rules;
    }

    public String getAttrValue(JXMLQuery jxmlQuery, String attrKey) {
        return jxmlQuery.attr(attrKey);
    }

    public String getFieldValue(JXMLQuery jxmlQuery, String fieldKey) {
        JXMLQuery.JXMLQueryList fields = jxmlQuery.children(fieldKey);
        if (fields == null || fields.isEmpty()) return null;
        String text = fields.text();
        return text;
    }

    public List<ExecuteRule> getExecuteRules(JXMLQuery jxmlQuery) {
        JXMLQuery.JXMLQueryList valids = jxmlQuery.children("valids");
        if (valids == null || valids.isEmpty()) return null;
        List<ExecuteRule> executeRules = new ArrayList<ExecuteRule>();
        for (JXMLQuery query : valids.children()) {
            ExecuteRule executeRule = new ExecuteRule();
            String execute = query.children("execute").text();
            String action = query.children("action").text();
            executeRule.setAction(action);
            executeRule.setExecute(execute);
            executeRules.add(executeRule);
        }
        return executeRules;
    }

    public RuleSet getRuleSet(JXMLQuery jxmlQuery) {
        String name = jxmlQuery.attr("name");
        RuleSet ruleSet = StringUtils.isEmpty(name) ? new RuleSet() : new RuleSet(name);
        JXMLQuery.JXMLQueryList children = jxmlQuery.children();
        if (children != null) {
            for (JXMLQuery child : children) {
                List<Rule> ruleList = getRules(child);
                if (ruleList != null && ruleList.size() > 0) {
                    ruleSet.addRule(ruleList);
                }
            }
        }
        return ruleSet;
    }

    @Override
    public RuleSet parse(String filePath, String ruleSetName) throws Exception {
        InputStream inputStream = new FileInputStream(new File(filePath));
        return parse(inputStream, ruleSetName);
    }

    @Override
    public RuleSet parse(InputStream inputStream, String ruleSetName) throws Exception {
        JXMLQuery jxmlQuery = JXMLQuery.load(inputStream);
        JXMLQuery.JXMLQueryList children = jxmlQuery.children();
        return getRuleSet(children.children(ruleSetName));
    }


    public static void main(String[] args) {
        try {
            Parser parser = new XmlParser();
            List<RuleSet> set = parser.parse(ClassUtil.getResourceAsInputStream("rule-test-content.xml"));

            System.out.println(set);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
