package com.datastory.banyan.validate.component.valid;

import com.datastory.banyan.validate.component.Component;
import com.datastory.banyan.validate.rule.ExecuteRule;
import com.datastory.banyan.validate.rule.Rule;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by abel.chan on 17/7/6.
 */
public class RegexValidComponent implements Component {

    public boolean execute(String field, Object value, Rule rule, ExecuteRule executeRule) {
        //获取regex的表达式
        String action = executeRule.getAction();
        Pattern pattern = Pattern.compile(action);
        // 忽略大小写的写法
        // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(value.toString());
        // 字符串是否与正则表达式相匹配
        return matcher.matches();
    }
}
