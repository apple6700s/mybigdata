package com.datastory.banyan.schema;

import java.io.Serializable;

/**
 * com.datatub.banyan.commons.schema.SchemaField
 *
 * @author lhfcws
 * @since 2017/5/19
 */
public class SchemaField implements Serializable {
    String field;
    String chField;
    String comment = null;
    String type = String.class.getCanonicalName();

    public SchemaField() {
    }

    public SchemaField(String field, String chField) {
        this.field = field;
        this.chField = chField;
    }

    public SchemaField(String field, String chField, String comment) {
        this.field = field;
        this.chField = chField;
        this.comment = comment;
    }

    public SchemaField(String field, String chField, String comment, String type) {
        this.field = field;
        this.chField = chField;
        this.comment = comment;
        this.type = type;
    }

    public SchemaField(SchemaField schemaField) {
        this.field = schemaField.getField();
        this.chField = schemaField.getChField();
        this.comment = schemaField.getComment();
        this.type = schemaField.getType();
    }

    public String getField() {
        return field;
    }

//    public void setField(String field) {
//        this.field = field;
//    }

    public String getChField() {
        return chField;
    }

//    public void setChField(String chField) {
//        this.chField = chField;
//    }

    public String getComment() {
        return comment;
    }

//    public void setComment(String comment) {
//        this.comment = comment;
//    }

    public String getType() {
        return type;
    }

//    public void setType(String type) {
//        this.type = type;
//    }

    public SchemaField clone() {
        return new SchemaField(this);
    }

    @Override
    public String toString() {
        return "SchemaField{" +
                "field='" + field + '\'' +
                ", chField='" + chField + '\'' +
                ", comment='" + comment + '\'' +
                ", type=" + type +
                '}';
    }
}
