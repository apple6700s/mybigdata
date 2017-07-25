package com.datastory.banyan.utils;

import com.datastory.banyan.validate.execute.impl.ValidExecute;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by abel.chan on 17/7/6.
 */
public class PropertiesUtils {
    public static Map<String, String> load(String filepath) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Map<String, String> result = Maps.newHashMap();
        InputStream resourceAsStream = ValidExecute.class.getResourceAsStream(filepath);
        Properties properties = new Properties();
        properties.load(resourceAsStream);
        if (properties.isEmpty()) {
            throw new NullPointerException("valid-execute-config file not exist mapping object !!!");
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            result.put(key, value);

        }
        return result;
    }
}
