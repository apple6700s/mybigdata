package com.datastory.banyan.validate.rule;

/**
 * Created by abel.chan on 17/7/10.
 */
public class ExecuteRule {

    String execute;
    String action;

    public ExecuteRule() {
    }

    public String getExecute() {
        return execute;
    }

    public void setExecute(String execute) {
        this.execute = execute;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return "ExecuteRule{" +
                "execute='" + execute + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}
