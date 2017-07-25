package com.datastory.banyan.spark;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.kafka.ToESKafkaProtocol;
import com.esotericsoftware.kryo.Kryo;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Tuple2;

/**
 * com.datastory.banyan.spark.BanyanKryoRegister
 *
 * @author lhfcws
 * @since 16/12/5
 */

public class BanyanKryoRegister implements KryoRegistrator {
    public static Class[] KRYO_CLASSES = {
            Params.class, String.class, Tuple2.class, ToESKafkaProtocol.class, StrParams.class,
            Boolean.class, Integer.class, Long.class, JSONObject.class, JSONArray.class
    };

    @Override
    public void registerClasses(Kryo kryo) {
        for (Class<?> klass : KRYO_CLASSES) {
            kryo.register(klass);
        }
    }
}
