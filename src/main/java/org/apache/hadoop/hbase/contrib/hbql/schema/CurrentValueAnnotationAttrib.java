package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.Util;
import org.apache.hadoop.hbase.contrib.hbql.client.Column;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

public class CurrentValueAnnotationAttrib extends FieldAttrib {

    public CurrentValueAnnotationAttrib(final Field field) throws HBqlException {
        super(field.getAnnotation(Column.class).family(),
              field.getAnnotation(Column.class).column(),
              field,
              FieldType.getFieldType(field),
              field.getAnnotation(Column.class).mapKeysAsColumns(),
              field.getAnnotation(Column.class).familyDefault(),
              field.getAnnotation(Column.class).getter(),
              field.getAnnotation(Column.class).setter());

        this.defineAccessors();

        if (isFinal(this.getField()))
            throw new HBqlException(this + "." + this.getField().getName() + " cannot have a @HColumn "
                                    + "annotation and be marked final");

        // Make sure type implements Map if this is true
        if (this.isMapKeysAsColumnsAttrib() && !Util.isParentClass(Map.class, this.getField().getType()))
            throw new HBqlException(this.getObjectQualifiedName() + " has @HColumn(mapKeysAsColumns=true) " +
                                    "annotation but doesn't implement the Map interface");
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
