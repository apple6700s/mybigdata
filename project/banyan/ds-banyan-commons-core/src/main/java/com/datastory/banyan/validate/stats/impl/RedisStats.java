package com.datastory.banyan.validate.stats.impl;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.redis.RedisConsts;
import com.datastory.banyan.redis.RedisDao;
import com.datastory.banyan.validate.engine.ValidEngine;
import com.datastory.banyan.validate.engine.impl.XmlValidEngine;
import com.datastory.banyan.validate.entity.DocType;
import com.datastory.banyan.validate.entity.FieldType;
import com.datastory.banyan.validate.entity.StatsResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.stats.Stats;
import com.datastory.banyan.validate.stats.StatsFactory;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by abel.chan on 17/7/7.
 */
public class RedisStats implements Stats {

    private RedisDao redisDao = RedisDao.getInstance(RhinoETLConfig.getInstance());

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    private static final Logger LOG = Logger.getLogger(RedisStats.class);


    @Override
    public boolean write(String projectName, StatsResult statsResult) {
        try {
            //doc
            ConcurrentHashMap<String, Long> docCount = statsResult.getDocCount();

            String docKey = getDocKey(projectName);
            for (Map.Entry<String, Long> entry : docCount.entrySet()) {
                redisDao.hincrBy(docKey, entry.getKey(), entry.getValue());
            }

            //field
            writeField(statsResult.getErrorFieldCount(), projectName, FieldType.ERROR);
            writeField(statsResult.getNullFieldCount(), projectName, FieldType.NULL);
            writeField(statsResult.getSuccessFieldCount(), projectName, FieldType.NORMAL);
            writeField(statsResult.getZeroFieldCount(), projectName, FieldType.ZERO);

            return true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    public boolean writeField(ConcurrentHashMap<String, Long> map, String projectName, FieldType fieldType) throws Exception {
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            redisDao.hincrBy(getFieldKey(projectName, entry.getKey()), fieldType.toString(), entry.getValue());
        }
        return true;
    }

    @Override
    public boolean write(String projectName, List<ValidResult> validResults) {
        {
            String docPrefixKey = getDocPrefix();
            Jedis jedis = null;
            try {
                jedis = redisDao.getJedis(docPrefixKey);
                Pipeline pipeline = jedis.pipelined();//使用管道进行加速
                for (ValidResult validResult : validResults) {
                    //doc
                    writeDoc(pipeline, projectName, validResult);
                }
                pipeline.sync();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return false;
            } finally {
                if (jedis != null) {
                    redisDao.returnJedis(docPrefixKey, jedis);
                }
            }
        }

        {
            String fieldPrefixKey = getFieldPrefix();
            Jedis jedis = null;
            try {
                jedis = redisDao.getJedis(fieldPrefixKey);
                Pipeline pipeline = jedis.pipelined();//使用管道进行加速
                for (ValidResult validResult : validResults) {
                    //field
                    writeField(pipeline, projectName, validResult);
                }
                pipeline.sync();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return false;
            } finally {
                if (jedis != null) {
                    redisDao.returnJedis(fieldPrefixKey, jedis);
                }
            }

        }
        return true;
    }

    @Override
    public boolean write(String projectName, ValidResult validResult) {
        {
            String docPrefixKey = getDocPrefix();
            Jedis jedis = null;
            try {
                jedis = redisDao.getJedis(docPrefixKey);
                Pipeline pipelined = jedis.pipelined();
                //doc
                writeDoc(pipelined, projectName, validResult);
                pipelined.sync();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return false;
            } finally {
                if (jedis != null) {
                    redisDao.returnJedis(docPrefixKey, jedis);
                }
            }
        }

        {
            String fieldPrefixKey = getFieldPrefix();
            Jedis jedis = null;
            try {
                jedis = redisDao.getJedis(fieldPrefixKey);
                Pipeline pipelined = jedis.pipelined();
                //field
                writeField(pipelined, projectName, validResult);
                pipelined.sync();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return false;
            } finally {
                if (jedis != null) {
                    redisDao.returnJedis(fieldPrefixKey, jedis);
                }
            }

        }
        return true;
    }

    /**
     * 将doc相关的参数存到redis中
     * 主要是 正常、异常可忽略，异常需剔除，所有
     *
     * @param projectName
     * @param validResult
     */
    public void writeDoc(Pipeline pipeline, String projectName, ValidResult validResult) {
        String key = getDocKey(projectName);
        //all
        writeDoc(pipeline, key, DocType.ALL, true);

        //normal
        writeDoc(pipeline, key, DocType.NORMAL, validResult.isSuccess());

        //error_ignore
        writeDoc(pipeline, key, DocType.ERROR_IGNORE, validResult.isErrorIgnore());

        //error_delete
        writeDoc(pipeline, key, DocType.ERROR_DELETE, validResult.isErrorDelete());
    }

    /**
     * 将doc相关的参数存到redis中
     *
     * @param key
     * @param docType
     * @param state
     */
    private void writeDoc(Pipeline pipeline, String key, DocType docType, boolean state) {
        try {
            if (state) {
                redisDao.hincrBy(pipeline, key, docType.toString(), 1);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }


    /**
     * 将field相关的参数存到redis中
     * 主要是 正常、异常、空值、零值
     *
     * @param projectName
     * @param validResult
     */
    public void writeField(Pipeline pipeline, String projectName, ValidResult validResult) {
        //zero
        writeField(pipeline, projectName, validResult.getZeroFields(), FieldType.ZERO);

        //null
        writeField(pipeline, projectName, validResult.getNullFields(), FieldType.NULL);

        //normal
        writeField(pipeline, projectName, validResult.getSuccessFields(), FieldType.NORMAL);

        //error
        writeField(pipeline, projectName, validResult.getErrorFields(), FieldType.ERROR);
    }

    /**
     * 将field相关的参数存到redis中
     *
     * @param projectName
     * @param fields
     * @param fieldType
     */
    private void writeField(Pipeline pipeline, String projectName, List<String> fields, FieldType fieldType) {
        try {
            if (fields != null && fields.size() > 0) {
                for (String field : fields) {
                    redisDao.hincrBy(pipeline, getFieldKey(projectName, field), fieldType.toString(), 1);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private String getNowDate() {
        return sdf.format(new Date());
    }

    private String getDocPrefix() {
        return RedisConsts.VALID_MON_KEY + "doc";
    }

    private String getDocKey(String projectName) {
        return getDocPrefix() + ":" + getNowDate() + ":" + projectName;
    }

    private String getFieldKey(String projectName, String field) {
        return getFieldPrefix() + ":" + getNowDate() + ":" + projectName + ":" + field;
    }

    private String getFieldPrefix() {
        return RedisConsts.VALID_MON_KEY + "field";
    }


    public static void main(String[] args) {
        try {
            InputStream inputStream = RedisStats.class.getResourceAsStream("/rule-test-content.xml");
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
