package com.datastory.banyan.validate.parser;

import com.datastory.banyan.validate.rule.RuleSet;

import java.io.InputStream;
import java.util.List;

/**
 * Created by abel.chan on 17/7/5.
 */
public interface Parser {
    List<RuleSet> parse(String filePath) throws Exception;

    List<RuleSet> parse(InputStream inputStream) throws Exception;

    RuleSet parse(String filePath, String ruleSetName) throws Exception;

    RuleSet parse(InputStream inputStream, String ruleSetName) throws Exception;
}
