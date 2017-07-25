package com.datastory.banyan.validate.entity;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by abel.chan on 17/7/6.
 */
public class ExecuteResult {

    private Map<String, Boolean> fieldState;

//    Map<String, Boolean> fieldNullState;
//
//    Map<String, Boolean> fieldZeroState;

    public ExecuteResult() {
        fieldState = Maps.newHashMap();
    }

    public void addElement(String key, Boolean state) throws NullPointerException, IllegalArgumentException {
        if (!fieldState.containsKey(key)) {
            fieldState.put(key, state);
        }
    }

//    public Map<String, Boolean> getMap(ExecuteResultType type) {
//        switch (type) {
//            case VALID:
//                return fieldValidState;
//            case NULL:
//                return fieldNullState;
//            case ZERO:
//                return fieldZeroState;
//        }
//        throw new IllegalArgumentException("ExecuteResultType param is wrong !!!!");
//    }
//
//    public enum ExecuteResultType {
//        VALID, NULL, ZERO
//    }


    public Map<String, Boolean> getFieldState() {
        return fieldState;
    }

    public void setFieldState(Map<String, Boolean> fieldState) {
        this.fieldState = fieldState;
    }
}
