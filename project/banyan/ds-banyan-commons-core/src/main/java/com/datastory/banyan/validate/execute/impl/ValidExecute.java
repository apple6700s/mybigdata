package com.datastory.banyan.validate.execute.impl;

import com.datastory.banyan.validate.component.Component;
import com.datastory.banyan.validate.entity.ErrorLevel;
import com.datastory.banyan.validate.entity.ExecuteResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.execute.Execute;
import com.datastory.banyan.validate.rule.ExecuteRule;
import com.datastory.banyan.validate.rule.Rule;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by abel.chan on 17/7/5.
 */
public class ValidExecute extends Execute {

    private static final Logger LOG = Logger.getLogger(ValidExecute.class);

    private static ValidExecute _instance;

    public ValidExecute() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        init("/valid-execute-config.properties");
    }

    public static ValidExecute getInstance() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
        if (_instance == null) {
            synchronized (ValidExecute.class) {
                if (_instance == null) {
                    _instance = new ValidExecute();
                }
            }
        }
        return _instance;
    }

    public ExecuteResult execute(Params params, Rule rule) throws Exception {

        //开始写执行逻辑
        ExecuteResult result = new ExecuteResult();
        if (rule == null || StringUtils.isEmpty(rule.getField())) {
            throw new NullPointerException("fields can not be null !!!");
        }
        if (rule.getExecuteRules() != null && rule.getExecuteRules().size() > 0) {
            for (ExecuteRule executeRule : rule.getExecuteRules()) {
                String execute = executeRule.getExecute().toLowerCase();
                if (!execute2Obj.containsKey(execute)) {
                    throw new IllegalArgumentException("valid execute do not support !!! support list:" + execute2Obj.keySet());
                }
                Component executeComponent = execute2Obj.get(execute);
                String field = rule.getField();
                if (params.containsKey(field)) {
                    Object value = params.get(field);
                    boolean isSuccess = executeComponent.execute(field, value, rule, executeRule);
                    result.addElement(field, isSuccess);
                    if (!isSuccess && rule.getErrorLevel().equals(ErrorLevel.ERROR_IGNORE_NULL)) {
                        //需要将params对应的字段置空
                        params.put(field, null);
                    }
                    if (!isSuccess) {
                        //说明一个规则出现错误，不需再往下进行。
                        break;
                    }
                }
            }
        } else {
            //说明没有检查的规则，所有的field都通过valid。
            result.addElement(rule.getField(), true);
        }
        return result;
    }

    @Override
    public void writeResult(Rule rule, ExecuteResult executeResult, ValidResult validResult) throws Exception {
        Map<String, Boolean> validMap = executeResult.getFieldState();
        //验证有值的数据
        for (Map.Entry<String, Boolean> entry : validMap.entrySet()) {
            String field = entry.getKey();
            Boolean isSuccess = entry.getValue();
            if (!isSuccess) {
                validResult.setErrorLevel(rule.getErrorLevel());
//                if (rule.errorState()) {
//                    //出现不可忽略的错误，需要将其删除。
//                    validResult.setErrorDelete(true);
//                } else {
//                    //说明可以被忽略
//                    validResult.setErrorIgnore(true);
//                }
                //说明验证不通过
                validResult.addErrorField(field);
            } else {
                validResult.addSuccessField(field);
            }
        }
    }
}
