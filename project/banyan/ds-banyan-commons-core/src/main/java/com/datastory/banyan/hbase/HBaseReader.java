package com.datastory.banyan.hbase;

import com.datastory.banyan.async.Async;
import com.datastory.banyan.async.AsyncPool;
import com.datastory.banyan.async.AsyncRet;
import com.datastory.banyan.async.Function;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.io.DataSourceReader;
import com.datastory.banyan.io.DataSrcReadHook;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.hbase.HBaseReader
 * 其中的hook输入参数要判断 类型，可以考虑用 @see com.datastory.banyan.utils.PatternMatch
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class HBaseReader implements DataSourceReader {
    protected List<DataSrcReadHook> hooks = new LinkedList<>();
    protected String family = "r";
    protected List<String> fields = new LinkedList<>();
    protected List<Get> cache = Collections.synchronizedList(new LinkedList<Get>());
    protected int maxCache = 100;
    protected String table;

    public HBaseReader(String table) {
        this.table = table;
    }

    public HBaseReader() {
    }

    public String getTable() {
        return table;
    }

    public int getMaxCache() {
        return maxCache;
    }

    public void setMaxCache(int maxCache) {
        this.maxCache = maxCache;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public List<DataSrcReadHook> getHooks() {
        return hooks;
    }

    public Get makeExistGet(String pk) {
        Get get = new Get(pk.getBytes());
        get.addColumn("r".getBytes(), "update_date".getBytes());
        return get;
    }

    public Get makeGet(String pk) {
        Get get = new Get(pk.getBytes());
        if (!CollectionUtil.isEmpty(fields)) {
            for (String field : fields) {
                get.addColumn(family.getBytes(), field.getBytes());
            }
        } else
            get.addFamily(family.getBytes());
        return get;
    }

    public Params result2Params(Result result) {
        return new ResultRDocMapper(result).map();
    }

    /**
     * @param pk
     * @return null 为 还未flush状态， List为正常返回结果
     * @throws Exception
     */
    public List<Params> batchRead(String pk) throws Exception {
        Get get = makeGet(pk);
        cache.add(get);

        if (cache.size() >= maxCache) {
            synchronized (this.getClass()) {
                if (cache.size() >= maxCache) {
                    List<Get> glist = new LinkedList<>(cache);
                    cache.clear();
                    return flush(glist);
                }
            }
        }

        return null;
    }

    /**
     * @param get
     * @return null 为 还未flush状态， List为正常返回结果
     * @throws Exception
     */
    public List<Params> batchRead(Get get) throws Exception {
        cache.add(get);

        if (cache.size() >= maxCache) {
            synchronized (this.getClass()) {
                if (cache.size() >= maxCache) {
                    List<Get> glist = new LinkedList<>(cache);
                    cache.clear();
                    return flush(glist);
                }
            }
        }

        return null;
    }

    public List<Params> flush() throws Exception {
        if (cache.size() > 0) {
            synchronized (this.getClass()) {
                if (cache.size() > 0) {
                    List<Get> glist = new LinkedList<>(cache);
                    cache.clear();
                    return flush(glist);
                }
            }
        }
        return null;
    }

    public List<Params> flush( final List<Get> clist) throws Exception {
        if (clist.size() > 0) {
            final List<Params> ret = new LinkedList<>();
            HTableInterface hTableInterface = HTableInterfacePool.get(getTable());
            try {
                for (final DataSrcReadHook dataSrcReadHook : getHooks())
                    Async.async(AsyncPool.MONITOR_POOL, new Function() {
                        @Override
                        public AsyncRet call() throws Exception {
                            dataSrcReadHook.beforeRead(clist);
                            return null;
                        }
                    });

                Result[] results = hTableInterface.get(clist);
                for (Result result : results) {
                    if (result != null && !result.isEmpty())
                        ret.add(result2Params(result));
                    else
                        ret.add(null);
                }

                for (final DataSrcReadHook dataSrcReadHook : getHooks())
                    Async.async(AsyncPool.MONITOR_POOL, new Function() {
                        @Override
                        public AsyncRet call() throws Exception {
                            dataSrcReadHook.afterRead(clist, ret);
                            return null;
                        }
                    });
                return ret;
            } finally {
                hTableInterface.close();
            }
        } else
            return null;
    }

    @Override
    public Params read(Object input) throws IOException {
        final Get get;
        if (input instanceof String) {
            String pk = (String) input;
            get = makeGet(pk);
        } else if (input instanceof Get) {
            get = (Get) input;
        } else
            throw new IOException("Not validate input for read : " + input.getClass());

        HTableInterface hTableInterface = HTableInterfacePool.get(getTable());
        try {
            for (final DataSrcReadHook dataSrcReadHook : getHooks())
                Async.async(AsyncPool.MONITOR_POOL, new Function() {
                    @Override
                    public AsyncRet call() throws Exception {
                        dataSrcReadHook.beforeRead(get);
                        return null;
                    }
                });

            Result result = hTableInterface.get(get);
            Params p = null;
            if (!result.isEmpty())
                p = result2Params(result);

            final Params writeResponse = p;
            for (final DataSrcReadHook dataSrcReadHook : getHooks())
                Async.async(AsyncPool.MONITOR_POOL, new Function() {
                    @Override
                    public AsyncRet call() throws Exception {
                        dataSrcReadHook.afterRead(get, writeResponse);
                        return null;
                    }
                });

            return p;
        } finally {
            hTableInterface.close();
        }
    }

    @Override
    public List<Params> reads(Iterator iterator) throws Exception {
        List<Params> ret = new LinkedList<>();
        while (iterator.hasNext()) {
            String pk = (String) iterator.next();
            List<Params> _ret = batchRead(pk);
            if (_ret != null)
                ret.addAll(_ret);
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        String[] pks = {
                "0000149269389dd673f2de10e5f5c49d",
                "000022201a37768de1566f7eaa0012a6",
                "0000670e283fadd2e3486ffdda6a3433",
                "000088903a1d71de723e7bd852254193",
                "000180a7fcabc58ce030e67f6a97b358",
                "0001b791470a3143b7e1e487f9ee3a3a",
                "0001c6a5718311b8f0e8c3a0db8ac22d",
                "00023a9fdc6210022a682872b17486ea",
                "0002460029f77e60e781d1424f3e60c4",
                "00029913fb942e25dcb4fed7f9c333a3",
                "0002d7d7904ae2bb6000dbc10cf1aaa6",
                "0002ee213770f924558374e07850f93f",
                "00034fd3b29506b39da6f94c41cfc1df",
                "0003603dc812bce9ec2eb1c94c8be50a",
                "0003d26dc2e6b45dfe790a847c71cff6",
                "0004038f69876a51181e92bc2822265a",
                "0004311924add5951afd79d266a56f39",
                "000499b720034ccc9b2ec0d05cf69c57",
                "0004be1027d16e2cdca29ec44dc39888",
                "0004d99a7e49b2ef0ba18a42309eb243"
        };

        HBaseReader hBaseReader = new HBaseReader() {
            @Override
            public String getTable() {
                return "DS_BANYAN_NEWSFORUM_POST_TEST";
            }
        };
        for (String pk : pks)
            hBaseReader.batchRead(pk);
        List<Params> params = hBaseReader.flush();

        for (Params p : params)
            System.out.println(p);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
