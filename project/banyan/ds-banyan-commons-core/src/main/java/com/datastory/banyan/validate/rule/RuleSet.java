package com.datastory.banyan.validate.rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abel.chan on 17/7/5.
 */
public class RuleSet {
    //默认的rule名称
    public static String DEFAULT_RULE_NAME = "default";

    /**
     * 同种名称的规则集会自动合并.可不传，默认名称就叫default
     */
    private String name;

    /**
     * 存储规则列表
     */
    private List<Rule> rules;

    public RuleSet() {
        this(DEFAULT_RULE_NAME, new ArrayList<Rule>());
    }

    public RuleSet(String name) {
        this(name, new ArrayList<Rule>());
    }

    public RuleSet(List<Rule> rules) {
        this(DEFAULT_RULE_NAME, rules);
    }

    public RuleSet(String name, List<Rule> rules) {
        this.name = name;
        this.rules = rules;
    }

    public RuleSet(Rule rule) {
        this(DEFAULT_RULE_NAME, rule);
    }

    public RuleSet(String name, Rule rule) {
        this.name = name;
        addRule(rule);
    }

    public void addRule(Rule rule) {
        if (this.rules == null) {
            this.rules = new ArrayList<>();
        }
        this.rules.add(rule);
    }

    public void addRule(List<Rule> rules) {
        if (this.rules == null) {
            this.rules = new ArrayList<>();
        }
        this.rules.addAll(rules);
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Rule> getRules() {
        if (rules == null) {
            rules = new ArrayList<Rule>();
        }
        return rules;
    }

    public void setRules(List<Rule> rules) {
        this.rules = rules;
    }

    @Override
    public String toString() {
        return "RuleSet{" +
                "name='" + name + '\'' +
                ", rules=" + rules +
                '}';
    }
}
