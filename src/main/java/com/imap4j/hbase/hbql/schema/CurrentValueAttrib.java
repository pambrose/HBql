package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HColumn;
import com.imap4j.hbase.hbase.HPersistException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:17:59 PM
 */
public class CurrentValueAttrib extends FieldAttrib {


    public CurrentValueAttrib(final Field field) throws HPersistException {
        super(field,
              FieldType.getFieldType(field),
              field.getAnnotation(HColumn.class).family(),
              field.getAnnotation(HColumn.class).column(),
              field.getAnnotation(HColumn.class).getter(),
              field.getAnnotation(HColumn.class).setter(),
              field.getAnnotation(HColumn.class).mapKeysAsColumns());

        this.defineAccessors();

        if (isFinal(this.getField()))
            throw new HPersistException(this + "." + this.getField().getName() + " cannot have a @HColumn "
                                        + "annotation and be marked final");

        // Make sure type implements Map if this is true
        if (this.isMapKeysAsColumns() && (!Map.class.isAssignableFrom(this.getField().getType())))
            throw new HPersistException(this.getObjectQualifiedName() + " has @HColumn(mapKeysAsColumns=true) " +
                                        "annotation but doesn't implement the Map interface");
    }

    private HColumn getColumnAnno() {
        return this.getField().getAnnotation(HColumn.class);
    }

    @Override
    public boolean isKeyAttrib() {
        return this.getColumnAnno().key();
    }

    private static boolean isFinal(final Field field) {
        return Modifier.isFinal(field.getModifiers());
    }
}
