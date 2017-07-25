package com.datastory.banyan.validate.execute.impl;

import com.datastory.banyan.validate.component.Component;
import com.datastory.banyan.validate.entity.ExecuteResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.execute.Execute;
import com.datastory.banyan.validate.rule.Rule;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by abel.chan on 17/7/5.
 */
public class ZeroExecute extends Execute {

    private static final Logger LOG = Logger.getLogger(ZeroExecute.class);

    private static ZeroExecute _instance;

    public ZeroExecute() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        init("/zero-execute-config.properties");
    }

    public static ZeroExecute getInstance() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
        if (_instance == null) {
            synchronized (ZeroExecute.class) {
                if (_instance == null) {
                    _instance = new ZeroExecute();
                }
            }
        }
        return _instance;
    }

    public ExecuteResult execute(Params params, Rule rule) throws Exception {

        //开始写执行逻辑
        ExecuteResult result = new ExecuteResult();

        if (StringUtils.isEmpty(rule.getField())) {
            throw new NullPointerException("fields can not be null !!!");
        }if(!rule.isRecordZero()){
            //说明不需要执行zero
            return result;
        }
        String type = rule.getType().toLowerCase();

        if (!execute2Obj.containsKey(type)) {
            throw new IllegalArgumentException("zero execute do not support !!! support list:" + execute2Obj.keySet());
        }

        Component executeComponent = execute2Obj.get(type);
        String field = rule.getField();
        if (params.containsKey(field)) {
            Object value = params.get(field);
            boolean isSuccess = executeComponent.execute(field, value, rule, null);
            result.addElement(field, isSuccess);
        }

        return result;
    }

    @Override
    public void writeResult(Rule rule, ExecuteResult executeResult, ValidResult validResult) throws Exception {
        if (rule.isRecordZero()) {
            Map<String, Boolean> zeroMap = executeResult.getFieldState();
            for (Map.Entry<String, Boolean> entry : zeroMap.entrySet()) {
                String field = entry.getKey();
                Boolean isZero = entry.getValue();
                if (isZero) {
                    validResult.addZeroField(field);
                }
            }
        }
    }
}
