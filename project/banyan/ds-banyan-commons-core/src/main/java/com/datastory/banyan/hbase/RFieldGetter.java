package com.datastory.banyan.hbase;

import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.StrParams;
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
        if (fields.length > 0)
            this.fields.addAll(Arrays.asList(fields));
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

    public Map<String, String> get(Get get) throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        try {
            Result result = hTableInterface.get(get);
            if (result == null || result.isEmpty())
                return null;

            StrParams ret = new StrParams();
            if (!CollectionUtil.isEmpty(fields))
                for (String field : fields) {
                    String res = HBaseUtils.getValue(result, family, field.getBytes());
                    ret.put(field, res);
                }
            else
                return new ResultRDocMapper(result).mapStr();
            return ret;
        } finally {
            hTableInterface.close();
        }
    }

    public Map<String, String> get(String pk) throws IOException {
        Get get = new Get(pk.getBytes());
        for (String field : fields)
            get.addColumn(family, field.getBytes());
        return get(get);
    }

    public List<? extends Map<String, String>> get(List<String> pks) throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        try {
            List<StrParams> ret = new ArrayList<>();
            List<Get> gets = new LinkedList<>();
            int maxGets = 1000;
            for (String pk : pks) {
                Get get = new Get(pk.getBytes());
                for (String field : fields)
                    get.addColumn(family, field.getBytes());
                gets.add(get);

                if (gets.size() >= maxGets)
                    try {
                        List<StrParams> _ret = new ArrayList<>();
                        Result[] results = hTableInterface.get(gets);
                        for (Result result : results) {
                            if (result == null || result.isEmpty()) {
                                _ret.add(null);
                                continue;
                            }

                            StrParams p = new StrParams();
                            if (!CollectionUtil.isEmpty(fields))
                                for (String field : fields) {
                                    String res = HBaseUtils.getValue(result, family, field.getBytes());
                                    p.put(field, res);
                                }
                            else
                                p = new ResultRDocMapper(result).mapStr();
                            _ret.add(p);
                        }
                        gets.clear();
                        ret.addAll(_ret);
                    } catch (Exception e) {
                        ErrorUtil._LOG.error(e.getMessage(), e);
                        ret.add(null);
                    }
            }
            if (gets.size() > 0)
                try {
                    List<StrParams> _ret = new ArrayList<>();
                    Result[] results = hTableInterface.get(gets);
                    for (Result result : results) {
                        StrParams p = new StrParams();
                        if (!CollectionUtil.isEmpty(fields))
                            for (String field : fields) {
                                String res = HBaseUtils.getValue(result, family, field.getBytes());
                                p.put(field, res);
                            }
                        else
                            p = new ResultRDocMapper(result).mapStr();
                        _ret.add(p);
                    }
                    gets.clear();
                    ret.addAll(_ret);
                } catch (Exception e) {
                    ErrorUtil._LOG.error(e.getMessage(), e);
                    ret.add(null);
                }

            return ret;
        } finally {
            hTableInterface.close();
        }
    }
}
