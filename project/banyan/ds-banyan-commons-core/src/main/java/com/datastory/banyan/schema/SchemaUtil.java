package com.datastory.banyan.schema;

import com.datastory.banyan.schema.annotation.Ch;
import com.datastory.banyan.schema.annotation.Inherit;
import com.datastory.banyan.schema.annotation.Type;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * com.datatub.banyan.commons.schema.SchemaUtil
 *
 * @author lhfcws
 * @since 2017/5/19
 */
public class SchemaUtil {
    public static Schema fromFieldsOnly(List<String> fields) {
        Schema schema = new Schema();
        for (String field : fields) {
            schema.add(field);
        }
        return schema;
    }

    public static Schema fromSchema(Class<? extends ISchema> ... schemaClasses) throws Exception {
        ISchema[] schemas = new ISchema[schemaClasses.length];
        int i = -1;
        for (Class<? extends ISchema> klass : schemaClasses) {
            i++;
            Object o = klass.getConstructor().newInstance();
            schemas[i] = (ISchema) o;
        }
        return fromSchema(schemas);
    }

    public static Schema fromSchema(ISchema... schemas) {
        Schema ret = new Schema();

        HashSet<String> fieldSet = new HashSet<>();

        for (ISchema schema : schemas) {
            Field[] fields = schema.getClass().getFields();

            // check Inherit
            HashSet<String> includes = new HashSet<>();
            HashSet<String> excludes = new HashSet<>();
            boolean includeAll = false;
            boolean excludeAll = false;

            Inherit inherit = schema.getClass().getAnnotation(Inherit.class);
            if (inherit != null) {
                includeAll = inherit.include().equals("*");
                if (!includeAll && !inherit.include().equals("")) {
                    String[] includesArr = inherit.include().split(",");

                    for (String s : includesArr) {
                        includes.add(s.trim());
                    }
                }

                excludeAll = inherit.exclude().equals("*");
                if (!excludeAll && !inherit.exclude().equals("")) {
                    String[] excludesArr = inherit.exclude().split(",");

                    for (String s : excludesArr) {
                        excludes.add(s.trim());
                    }
                }
            } else
                includeAll = true;

            // fields
            for (Field field : fields) {
                String name = field.getName();
                boolean can = includeAll || includes.contains(name);
                can &= !(excludeAll || excludes.contains(name));

                if (can && !fieldSet.contains(name)) {
                    SchemaField schemaField = makeSchemaField(field);
                    ret.add(schemaField);
                    fieldSet.add(name);
                }
            }
        }

        return ret;
    }

    public static SchemaField makeSchemaField(Field field) {
        String fieldName = field.getName();
        String chFieldName = fieldName;
        String cmt = null;

        Ch ch = field.getAnnotation(Ch.class);
        Type type = field.getAnnotation(Type.class);
        if (ch != null) {
            chFieldName = ch.ch();
            cmt = ch.cmt();
        }

        if (type == null)
            return new SchemaField(fieldName, chFieldName, cmt);
        else
            return new SchemaField(fieldName, chFieldName, cmt, type.type().getCanonicalName());
    }

    public static Schema intersectFieldsNCreate(Schema s, Collection<String> fieldCollection) {
        int i = 0;
        HashSet<String> fieldSet = new HashSet<>(fieldCollection);

        Schema schema = new Schema();
        for (SchemaField schemaField : s) {
            if (fieldSet.contains(schemaField.getField())) {
                schema.add(schemaField);
            }
            i++;
        }

        return schema;
    }

    public static SchemaField findByField(Schema schema, String field) {
        if (field == null || schema == null)
            return null;
        for (SchemaField schemaField : schema) {
            if (field.equals(schemaField.getField()))
                return schemaField;
        }
        return null;
    }

    public static Schema fromJson(String json) {
        return FastJsonSerializer.deserialize(json, Schema.class);
    }
}
