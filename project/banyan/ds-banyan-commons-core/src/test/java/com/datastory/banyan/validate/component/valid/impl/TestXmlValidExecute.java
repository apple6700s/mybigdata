package com.datastory.banyan.validate.component.valid.impl;

import com.datastory.banyan.validate.engine.ValidEngine;
import com.datastory.banyan.validate.engine.impl.XmlValidEngine;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.stats.Stats;
import com.datastory.banyan.validate.stats.StatsFactory;
import com.yeezhao.commons.util.Entity.Params;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abel.chan on 17/7/5.
 */
public class TestXmlValidExecute {

    @Test
    public void testXmlValidExecute() {
        try {
            InputStream inputStream = this.getClass().getResourceAsStream("/rule-test-content.xml");

            ValidEngine validEngine = XmlValidEngine.getInstance(inputStream);

            Params params = new Params();
            params.put("id", "234");
            params.put("name", "23");

            params.put("time", "ddd ");

            System.out.println(params);
            ValidResult validResult = validEngine.execute(params);
            System.out.println(validResult.toString());
            System.out.println(params);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRedisStats() {
        try {
            InputStream inputStream = this.getClass().getResourceAsStream("/rule-test-content.xml");
            ValidEngine validEngine = XmlValidEngine.getInstance(inputStream);
            List<Params> list = new ArrayList<>();
            {
                Params params = new Params();
//            params.put("id", "234");
                params.put("mid", "123456");
                params.put("time", "abcd");
                list.add(params);
            }
            {
                Params params = new Params();
                params.put("id", "234");
                params.put("mid", "123456");
                params.put("time", "20170601000000");
                list.add(params);
            }

            Stats stats = StatsFactory.getStatsInstance();
            for (Params params : list) {
                System.out.println("[old]:" + params);
                ValidResult validResult = validEngine.execute(params);
                System.out.println("[valid result]:" + validResult.toString());
                System.out.println("[new]:" + params);
                stats.write("test", validResult);
            }
            System.out.println("执行完毕！！！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
