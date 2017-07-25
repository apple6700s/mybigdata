package com.datastory.commons3.es.lucene_writer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.lang.reflect.Type;

/**
 * FastJson is faster than Gson.
 * But DO remember your objects has get/set for the fields you want to serialze.
 * @author lhfcws
 */
public class FastJsonSerializer {

    /**
     * 把给定的对象序列化成json字符串
     * @param obj 给定的对象
     * @return 对象序列化后的json字符串
     */
    public static <T> String serialize(T obj) {
        return JSON.toJSONString(obj,
                SerializerFeature.IgnoreNonFieldGetter,
                SerializerFeature.SkipTransientField,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.BrowserCompatible
        );
    }

    public static <T> String serializePretty(T obj) {
        return JSON.toJSONString(obj,
                SerializerFeature.IgnoreNonFieldGetter,
                SerializerFeature.SkipTransientField,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.BrowserCompatible,
                SerializerFeature.PrettyFormat
        );
    }

    /**
     * 根据类名把json字符串反序列化成实体类对象
     * @param json 待反序列化的json字符串
     * @param klass 反序列化的实体类
     * @return 反序列化后的对象
     */
    public static <T> T deserialize(String json, Class<T> klass) {
        return JSON.parseObject(json, klass);
    }

    /**
     * 把Json字符串反序列化成实现了Type接口的实体类对象
     * @param json 待反序列化的json字符串
     * @param type 泛型类型
     * @return 反序列化后的对象
     */
    public static <T> T deserialize(String json, Type type) {
        return JSON.parseObject(json, type);
    }
}
