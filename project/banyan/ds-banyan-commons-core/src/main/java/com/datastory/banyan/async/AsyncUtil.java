package com.datastory.banyan.async;

import java.util.List;

/**
 * @author lhfcws
 * @since 16/1/12.
 */
public class AsyncUtil {
    public static Object[] wrapArgs(List<Object> list) {
        if (list == null) return null;

        Object[] arr = new Object[list.size()];
        list.toArray(arr);
        return arr;
    }

    public static Object[] wrapArgs(Object ... objs) {
        return objs;
    }
}
