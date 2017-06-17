package com.datastory.banyan.schema;

import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import java.util.ArrayList;
import java.util.Collection;

/**
 * com.datastory.banyan.schema.Schema
 *
 * @author lhfcws
 * @since 2017/5/27
 */
public class Schema extends ArrayList<SchemaField> {

    public Schema(int initialCapacity) {
        super(initialCapacity);
    }

    public Schema() {
    }

    public Schema(Collection<? extends SchemaField> c) {
        super(c);
    }

    public ArrayList<String> getFields() {
        ArrayList<String> fields = new ArrayList<>();
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
}
