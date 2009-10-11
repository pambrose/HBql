package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HColumn;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

public class CurrentValueAnnotationAttrib extends FieldAttrib {

    public CurrentValueAnnotationAttrib(final Field field) throws HBqlException {
        super(field.getAnnotation(HColumn.class).family(),
              field.getAnnotation(HColumn.class).column(),
              field,
              FieldType.getFieldType(field),
              field.getAnnotation(HColumn.class).mapKeysAsColumns(),
              field.getAnnotation(HColumn.class).getter(),
              field.getAnnotation(HColumn.class).setter());

        this.defineAccessors();

        if (isFinal(this.getField()))
            throw new HBqlException(this + "." + this.getField().getName() + " cannot have a @HColumn "
                                    + "annotation and be marked final");

        // Make sure type implements Map if this is true
        if (this.isMapKeysAsColumnsColumn() && !HUtil.isParentClass(Map.class, this.getField().getType()))
            throw new HBqlException(this.getObjectQualifiedName() + " has @HColumn(mapKeysAsColumns=true) " +
                                    "annotation but doesn't implement the Map interface");
    }

    private HColumn getColumnAnno() {
        return this.getField().getAnnotation(HColumn.class);
    }

    public boolean isKeyAttrib() {
        return this.getColumnAnno().key();
    }

    private static boolean isFinal(final Field field) {
        return Modifier.isFinal(field.getModifiers());
    }
}
