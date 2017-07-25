package com.datastory.banyan.validate.rule;

import com.datastory.banyan.validate.entity.ErrorLevel;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abel.chan on 17/7/5.
 */
public class Rule implements Comparable<Rule>, Cloneable {

    String field;
    ErrorLevel errorLevel;
    boolean isCanNull;
    String type;
    boolean isRecordZero;

    List<ExecuteRule> executeRules;

    String description;

    public Rule() {
    }

    public Rule(String field, String errorLevel, boolean isCanNull, String type, boolean isRecordZero, List<ExecuteRule> executeRules, String description) throws Exception {
        if (StringUtils.isNotEmpty(field)) {
            this.field = field;
        } else {
            throw new Exception("field can't be null !!!");
        }
        this.errorLevel = ErrorLevel.getErrorLevel(errorLevel);
        this.isCanNull = isCanNull;
        this.type = type;
        this.isRecordZero = isRecordZero;
        this.executeRules = executeRules;
        this.description = description;
    }


    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public int compareTo(Rule o) {
        ErrorLevel x = this.errorLevel;
        ErrorLevel y = o.errorLevel;
        return x.compareTo(y);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }


    public ErrorLevel getErrorLevel() {
        return errorLevel;
    }

    public void setErrorLevel(String errorLevel) {
        this.errorLevel = ErrorLevel.getErrorLevel(errorLevel);
    }

    public void setErrorLevel(ErrorLevel errorLevel) {
        this.errorLevel = errorLevel;
    }

    public boolean isCanNull() {
        return isCanNull;
    }

    public void setCanNull(boolean canNull) {
        isCanNull = canNull;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isRecordZero() {
        return isRecordZero;
    }

    public void setRecordZero(boolean recordZero) {
        isRecordZero = recordZero;
    }

    public List<ExecuteRule> getExecuteRules() {
        return executeRules;
    }

    public void setExecuteRules(List<ExecuteRule> executeRules) {
        this.executeRules = executeRules;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * 默认级别是忽略置空
     *
     * @return
     */
    public boolean errorState() {
        if (errorLevel == null || errorLevel.equals(ErrorLevel.ERROR_IGNORE_NULL)) {
            return false;
        }
        return true;
    }

    public boolean isRuleValid() {
//        if (executeRules == null || executeRules.size() <= 0) {
//            return false;
//        }
        if (StringUtils.isEmpty(field)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "field='" + field + '\'' +
                ", errorLevel=" + errorLevel +
                ", isCanNull=" + isCanNull +
                ", type='" + type + '\'' +
                ", isRecordZero=" + isRecordZero +
                ", executeRules=" + executeRules +
                ", description='" + description + '\'' +
                '}';
    }

    public Rule clone() {
        Rule rule = new Rule();
        rule.setCanNull(this.isCanNull);
        rule.setDescription(this.description);
        rule.setErrorLevel(this.errorLevel);
        if (executeRules != null) {
            rule.setExecuteRules(new ArrayList<ExecuteRule>(this.executeRules));
        }
        rule.setType(this.type);
        rule.setField(this.field);
        rule.setRecordZero(this.isRecordZero);

        return rule;
    }
}
