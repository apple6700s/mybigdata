package com.datastory.banyan.validate.execute;

import com.datastory.banyan.validate.component.Component;
import com.datastory.banyan.validate.entity.ExecuteResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.rule.Rule;
import com.datastory.banyan.utils.PropertiesUtils;
import com.google.common.collect.Maps;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by abel.chan on 17/7/6.
 */
public abstract class Execute {

    private static final Logger LOG = Logger.getLogger(Execute.class);

    /**
     * 存储execute名称和validaExecute之间的映射
     */
    public Map<String, Component> execute2Obj = Maps.newHashMap();

    /**
     * 初始化，
     * 1.execute名称和obj之间的映射关系
     */
    protected void init(String filepath) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        LOG.info("start init the mapping of execute name to real obj !!!");

        Map<String, String> load = PropertiesUtils.load(filepath);

        for (Map.Entry<String, String> entry : load.entrySet()) {
            String key = entry.getKey();
            if (!execute2Obj.containsKey(key)) {
                Class classType = Class.forName(entry.getValue());
                Component obj = (Component) classType.newInstance();
                execute2Obj.put(key.toLowerCase(), obj);
            }
        }
        LOG.info("end init the mapping of execute name to real obj !!!");
    }

    /**
     *
     * 执行相对于的execute逻辑
     *
     * @param params
     * @param rule
     * @return
     * @throws Exception
     */
    public abstract ExecuteResult execute(Params params, Rule rule) throws Exception;

    /**
     * 将对应execute的result写入到validResult中
     * @param rule
     * @param executeResult
     * @param validResult 将被修改
     * @throws Exception
     */
    public abstract void writeResult(Rule rule, ExecuteResult executeResult, ValidResult validResult) throws Exception;
}
