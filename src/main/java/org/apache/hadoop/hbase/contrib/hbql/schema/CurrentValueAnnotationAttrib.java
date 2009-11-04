package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.hadoop.hbase.contrib.hbql.client.Column;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class CurrentValueAnnotationAttrib extends FieldAttrib {

    public CurrentValueAnnotationAttrib(final Field field) throws HBqlException {
        super(field.getAnnotation(Column.class).family(),
              field.getAnnotation(Column.class).column(),
              field,
              FieldType.getFieldType(field),
              field.getAnnotation(Column.class).familyDefault(),
              field.getAnnotation(Column.class).getter(),
              field.getAnnotation(Column.class).setter());

        this.defineAccessors();

        if (isFinal(this.getField()))
            throw new HBqlException(this + "." + this.getField().getName() + " cannot have a @Column "
                                    + "annotation and be marked final");
    }

    private Column getColumnAnno() {
        return this.getField().getAnnotation(Column.class);
    }

    public boolean isAKeyAttrib() {
        return this.getColumnAnno().key();
    }

    private static boolean isFinal(final Field field) {
        return Modifier.isFinal(field.getModifiers());
    }
}
