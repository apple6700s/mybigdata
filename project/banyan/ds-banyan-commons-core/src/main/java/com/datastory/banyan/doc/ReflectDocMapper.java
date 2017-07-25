package com.datastory.banyan.doc;

import weibo4j.model.Status;

import java.lang.reflect.Field;

/**
 * com.datastory.banyan.doc.ReflectDocMapper
 *
 * @author lhfcws
 * @since 16/11/23
 */

public abstract class ReflectDocMapper<T> extends DocMapper {
    protected T in;

    public ReflectDocMapper(T in) {
        this.in = in;
    }

    @Override
    public String getString(String key) {
        if (in != null)
            try {
                Field field = in.getClass().getDeclaredField(key);
                field.setAccessible(true);
                return String.valueOf(field.get(in));
            } catch (Exception e) {
                e.printStackTrace();
            }
        return null;
    }

    @Override
    public Integer getInt(String key) {
        if (in != null)
            try {
                Field field = in.getClass().getDeclaredField(key);
                field.setAccessible(true);
                return field.getInt(in);
            } catch (Exception e) {
                e.printStackTrace();
            }
        return null;
    }
}
