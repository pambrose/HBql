package com.imap4j.hbase;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public class FieldAttrib {

    private final Field field;
    private final String family;
    private final String column;
    private final String lookup;

    private Method lookupMethod = null;


    public FieldAttrib(final Class clazz, final Field field, final Column column) throws PersistException {

        this.field = field;
        this.family = column.family();
        this.column = column.column().length() > 0 ? column.column() : this.getField().getName();
        this.lookup = column.lookup();

        try {
            if (this.lookup.length() > 0)
                this.lookupMethod = clazz.getDeclaredMethod(this.lookup);
        }
        catch (NoSuchMethodException e) {
            throw new PersistException("Missing method "
                                       + clazz.getSimpleName()
                                       + "." + this.lookupMethod);
        }

    }

    public String getFamily() {
        return this.family;
    }

    public String getColumn() {
        return column;
    }

    public Field getField() {
        return field;
    }
}
