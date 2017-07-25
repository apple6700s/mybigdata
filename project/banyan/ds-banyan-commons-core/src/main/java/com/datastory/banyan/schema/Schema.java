package com.datastory.banyan.schema;

import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * com.datastory.banyan.schema.Schema
 *
 * @author lhfcws
 * @since 2017/5/27
 */
public class Schema implements Iterable<SchemaField>, Serializable {
    public static final long serialVersionUID = 20170614l;

    private ArrayList<SchemaField> schemaFields = new ArrayList<>();

    public Schema() {
    }

    public Schema(Collection<? extends SchemaField> c) {
        this.addAll(c);
    }

    public Schema(Iterable<? extends SchemaField> c) {
        this.addAll(c);
    }

    public Schema(Schema schema) {
        for (SchemaField schemaField : schema) {
            this.add(schemaField);
        }
    }

    public boolean isEmpty() {
        return this.schemaFields.isEmpty();
    }

    public ArrayList<String> getFields() {
        ArrayList<String> fields = new ArrayList<>();
        for (SchemaField schemaField : this) {
            fields.add(schemaField.getField());
        }
        return fields;
    }

    public HashSet<String> getFieldSet() {
        HashSet<String> fields = new HashSet<>();
        for (SchemaField schemaField : this) {
            fields.add(schemaField.getField());
        }
        return fields;
    }

    public ArrayList<String> getChFields() {
        ArrayList<String> fields = new ArrayList<>();
        for (SchemaField schemaField : this) {
            fields.add(schemaField.getChField());
        }
        return fields;
    }

    public ArrayList<String> getComments() {
        ArrayList<String> fields = new ArrayList<>();
        for (SchemaField schemaField : this) {
            fields.add(schemaField.getComment());
        }
        return fields;
    }

    public ArrayList<String> getFieldTypes() {
        ArrayList<String> fields = new ArrayList<>();
        for (SchemaField schemaField : this) {
            fields.add(schemaField.getType());
        }
        return fields;
    }

    public SchemaField get(int i) {
        return schemaFields.get(i);
    }

    public int size() {
        return schemaFields.size();
    }

    public Schema add(SchemaField schemaField) {
        this.schemaFields.add(schemaField);
        return this;
    }

    public Schema addAll(Collection<? extends SchemaField> c) {
        this.schemaFields.addAll(c);
        return this;
    }

    public Schema addAll(Iterable<? extends SchemaField> c) {
        for (SchemaField schemaField : c)
            this.add(schemaField);
        return this;
    }

    public Schema remove(int i) {
        this.schemaFields.remove(i);
        return this;
    }

    public String getField(int i) {
        return get(i).getField();
    }

    public String getChField(int i) {
        return get(i).getChField();
    }

    public String getComment(int i) {
        return get(i).getComment();
    }

    public String getFieldType(int i) {
        return get(i).getType();
    }

    public String toString() {
        return FastJsonSerializer.serialize(this);
    }

    public synchronized Schema remove(String field) {
        int removeIndex = -1;
        for (int i = 0; i < size(); i++) {
            if (get(i).getField().equals(field)) {
                removeIndex = i;
                break;
            }
        }
        if (removeIndex != -1) {
            this.remove(removeIndex);
        }
        return this;
    }

    public Schema add(String field, String chField, String comment, Class type) {
        if (chField == null)
            chField = field;

        add(new SchemaField(field, chField, comment, type.getCanonicalName()));

        return this;
    }

    public Schema add(String field, String chField, String comment, String type) {
        if (chField == null)
            chField = field;

        add(new SchemaField(field, chField, comment, type));

        return this;
    }

    public Schema add(String field, String chField, String comment) {
        if (chField == null)
            chField = field;

        add(new SchemaField(field, chField, comment));

        return this;
    }

    public Schema add(String field, String chField) {
        return add(field, chField, null);
    }

    public Schema add(String field) {
        return add(field, null);
    }

    public Schema merge(Schema other) {
        Schema addSchema = new Schema();
        for (SchemaField field2 : other) {
            boolean toAdd = true;
            for (SchemaField field1 : this) {
                if (field1.getField().equals(field2.getField())) {
                    toAdd = false;
                    break;
                }
            }
            if (toAdd) {
                addSchema.add(field2);
            }
        }

        this.addAll(addSchema);

        return this;
    }

    public Schema clone() {
        return new Schema(this);
    }

    @Override
    public Iterator<SchemaField> iterator() {
        return this.schemaFields.iterator();
    }

    public ArrayList<SchemaField> getSchemaFields() {
        return schemaFields;
    }
}
