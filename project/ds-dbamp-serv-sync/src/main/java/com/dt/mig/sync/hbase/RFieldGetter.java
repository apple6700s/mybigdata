package com.dt.mig.sync.hbase;

import com.dt.mig.sync.hbase.doc.Result2DocMapper;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * 单独对某些字段进行get
 * <p>
 * com.datastory.banyan.hbase.RFieldGetter
 */
public class RFieldGetter implements Serializable {
    private String table;
    private List<String> fields = new LinkedList<>();
    private byte[] family = "r".getBytes();

    public RFieldGetter(String table, String field) {
        this.table = table;
        this.fields.add(field);
    }

    public RFieldGetter(String table, String... fields) {
        this.table = table;
        this.fields.addAll(Arrays.asList(fields));
    }

    public RFieldGetter(String table, Collection<String> fields) {
        this.table = table;
        this.fields.addAll(fields);
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public String singleGet(Get get, String field) throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        try {
            Result result = hTableInterface.get(get);
            return HBaseUtils.getValue(result, family, field.getBytes());
        } finally {
            hTableInterface.close();
        }
    }

    public Params get(String pk) throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        try {
            Get get = new Get(pk.getBytes());
            for (String field : fields)
                get.addColumn(family, field.getBytes());
            Result result = hTableInterface.get(get);
            Params ret = new Params();
            if (!CollectionUtil.isEmpty(fields)) for (String field : fields) {
                String res = HBaseUtils.getValue(result, family, field.getBytes());
                ret.put(field, res);
            }
            else return new Result2DocMapper(result).map();
            return ret;
        } finally {
            hTableInterface.close();
        }
    }

    public Params get(Get get) throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        try {
            for (String field : fields)
                get.addColumn(family, field.getBytes());
            Result result = hTableInterface.get(get);
            Params ret = new Params();
            if (!CollectionUtil.isEmpty(fields)) for (String field : fields) {
                String res = HBaseUtils.getValue(result, family, field.getBytes());
                ret.put(field, res);
            }
            else return new Result2DocMapper(result).map();
            return ret;
        } finally {
            hTableInterface.close();
        }
    }

    public List<? extends Map<String, ? extends Object>> get(List<String> pks) throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        try {
            List<Params> ret = new ArrayList<>();
            List<Get> gets = new LinkedList<>();
            int maxGets = 1000;
            for (String pk : pks) {
                Get get = new Get(pk.getBytes());
                for (String field : fields)
                    get.addColumn(family, field.getBytes());
                gets.add(get);

                if (gets.size() >= maxGets) try {
                    List<Params> _ret = new ArrayList<>();
                    Result[] results = hTableInterface.get(gets);
                    for (Result result : results) {
                        Params p = new Params();
                        if (!CollectionUtil.isEmpty(fields)) {
                            for (String field : fields) {
                                String res = HBaseUtils.getValue(result, family, field.getBytes());
                                p.put(field, res);
                            }
                            _ret.add(p);
                        } else _ret.add(new Result2DocMapper(result).map());
                    }
                    gets.clear();
                    ret.addAll(_ret);
                } catch (Exception e) {
                    ret.add(null);
                    e.printStackTrace();
                }
            }
            if (gets.size() > 0) try {
                List<Params> _ret = new ArrayList<>();
                Result[] results = hTableInterface.get(gets);
                for (Result result : results) {
                    Params p = new Params();
                    if (!CollectionUtil.isEmpty(fields)) {
                        for (String field : fields) {
                            String res = HBaseUtils.getValue(result, family, field.getBytes());
                            p.put(field, res);
                        }
                        _ret.add(p);
                    } else _ret.add(new Result2DocMapper(result).map());
                }
                gets.clear();
                ret.addAll(_ret);
            } catch (Exception e) {
                ret.add(null);
                e.printStackTrace();
            }

            return ret;
        } finally {
            hTableInterface.close();
        }
    }
}
