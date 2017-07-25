package com.datastory.banyan.validate.component.valid;

import com.datastory.banyan.validate.component.Component;
import com.datastory.banyan.validate.rule.ExecuteRule;
import com.datastory.banyan.validate.rule.Rule;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by abel.chan on 17/7/6.
 */
public class DateValidComponent implements Component {

    public boolean execute(String field, Object value, Rule rule, ExecuteRule executeRule) {

        SimpleDateFormat sdf = new SimpleDateFormat(executeRule.getAction());
        try {
            sdf.parse(value.toString());
            return true;
        } catch (ParseException e) {
            return false;
        }

    }

    public static void main(String[] args) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            sdf.parse("20170601000000");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
