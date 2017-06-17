package com.datastory.banyan.utils;

import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * com.datatub.rhino.utils.Args
 *
 * @author lhfcws
 * @since 2016/11/3
 */
public class Args implements Serializable {
    private List<Object> args = new ArrayList<>();

    public Args(Object ... arr) {
        args.addAll(Arrays.asList(arr));
    }

    public Args(Args args1) {
        args.addAll(args1.args);
    }

    public Args() {
    }

    public void setArgs(List<Object> args) {
        this.args = args;
    }

    public Object[] getArgs() {
        Object[] arr = new Object[args.size()];
        arr = args.toArray(arr);
        return arr;
    }

    public Args addArgs(Object obj) {
        args.add(obj);
        return this;
    }

    public Args clear() {
        this.args.clear();
        return this;
    }

    public Object get(int i) {
        if (i >= args.size())
            throw new ArrayIndexOutOfBoundsException("current index " + i + " out of args size " + args.size());
        return args.get(i);
    }

    public Params link(String[] fields) {
        Params p = new Params();
        int i = 0;
        for (Object value : args) {
            p.put(fields[i], value);
            i++;
        }
        return p;
    }

    public Params link(List<String> fields) {
        Params p = new Params();
        int i = 0;
        for (Object value : args) {
            p.put(fields.get(i), value);
            i++;
        }
        return p;
    }

    @Override
    public String toString() {
        return StringUtils.join(args, ",");
    }

    public static List<Args> createThreadSafeList() {
        return Collections.synchronizedList(new ArrayList<Args>());
    }

    public static ArgsList createArgsList() {
        return new ArgsList();
    }

    public static List<Object[]> toObjectArrays(List<Args> argsList) {
        ArrayList<Object[]> params = new ArrayList<>();
        for (Args args : argsList) {
            params.add(args.getArgs());
        }
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Args args1 = (Args) o;

        return args != null ? args.equals(args1.args) : args1.args == null;
    }

    @Override
    public int hashCode() {
        return args != null ? args.hashCode() : 0;
    }

    public static class ArgsList extends ArrayList<Args> {
        public ArgsList() {
        }

        public ArgsList(Collection<? extends Args> c) {
            super(c);
        }
    }
}
