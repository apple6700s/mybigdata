package com.datastory.banyan.validate.component.zero;

import com.datastory.banyan.validate.component.Component;
import com.datastory.banyan.validate.rule.ExecuteRule;
import com.datastory.banyan.validate.rule.Rule;

/**
 * Created by abel.chan on 17/7/6.
 */
public class StringZeroComponent implements Component {
    @Override
    public boolean execute(String field, Object value, Rule rule, ExecuteRule executeRule) {
        try {
            if (value.toString().trim().length() <= 0) {
                return true;
            }
        } catch (Exception ex) {

        }
        return false;
    }
}
