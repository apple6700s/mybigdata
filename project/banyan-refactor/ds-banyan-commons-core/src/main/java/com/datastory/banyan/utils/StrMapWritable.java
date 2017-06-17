package com.datastory.banyan.utils;

import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.io.HadoopRPCUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * com.datastory.banyan.utils.StrMapWritable
 *
 * @author lhfcws
 * @since 2017/4/1
 */
public class StrMapWritable implements Writable {

    protected HashMap<String, String> mp = null;

    public StrMapWritable() {
    }

    public StrMapWritable(HashMap<String, ? extends Object> mp1) {
        mp = new HashMap<>();
        for (Map.Entry<String, ? extends Object> e : mp1.entrySet()) {
            if (e.getValue() != null)
                this.mp.put(e.getKey(), e.getValue().toString());
        }
    }

    public HashMap<String, String> getMp() {
        return mp;
    }

    public void setMp(HashMap<String, String> mp) {
        this.mp = mp;
    }

    public Params toParams() {
        return new Params(mp);
    }

    public StrParams toStrParams() {
        return new StrParams(mp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (mp != null) {
            out.writeInt(mp.size());
            for (Map.Entry<String, String> e : mp.entrySet()) {
                HadoopRPCUtil.writerUTF(out, e.getKey());
                HadoopRPCUtil.writerUTF(out, e.getValue());
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.mp = new HashMap<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = HadoopRPCUtil.readUTF(in);
            String value = HadoopRPCUtil.readUTF(in);
            this.mp.put(key, value);
        }
    }
}
