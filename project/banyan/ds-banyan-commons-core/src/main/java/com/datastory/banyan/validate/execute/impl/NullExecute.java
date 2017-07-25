package com.datastory.banyan.validate.execute.impl;

import com.datastory.banyan.validate.entity.ExecuteResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.execute.Execute;
import com.datastory.banyan.validate.rule.Rule;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by abel.chan on 17/7/5.
 */
public class NullExecute extends Execute {

    private static final Logger LOG = Logger.getLogger(NullExecute.class);

    private static NullExecute _instance;

    public NullExecute() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    }

    public static NullExecute getInstance() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
        if (_instance == null) {
            synchronized (NullExecute.class) {
                if (_instance == null) {
                    _instance = new NullExecute();
                }
            }
        }
        return _instance;
    }

    public ExecuteResult execute(Params params, Rule rule) throws Exception {

        //开始写执行逻辑
        ExecuteResult result = new ExecuteResult();

        String field = rule.getField();
        if (!params.containsKey(field)) {
            if (!rule.isCanNull()) {
                //说明不能为空
                result.addElement(field, false);
            } else {
                //说明允许字段为空
                result.addElement(field, true);
            }
        }

        return result;
    }

    @Override
    public void writeResult(Rule rule, ExecuteResult executeResult, ValidResult validResult) throws Exception {
        //验证空值的数据
        Map<String, Boolean> nullMap = executeResult.getFieldState();
        for (Map.Entry<String, Boolean> entry : nullMap.entrySet()) {
            String field = entry.getKey();
            Boolean isCanNull = entry.getValue();
            if (!isCanNull) {
                //根据错误级别进行设置，若errorLevel为空时，则使用ERROR_IGNORE
                validResult.setErrorLevel(rule.getErrorLevel());
            }
            //说明该值为null。
            validResult.addNullField(field);
        }
    }
}
