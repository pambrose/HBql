package com.imap4j.hbase;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public class FieldAttrib {

    private final Class clazz;
    private final Field field;
    private final String family;
    private final String column;
    private final String lookup;
    private final Column.Strategy strategy;

    private Method lookupMethod = null;


    public FieldAttrib(final Class clazz, final Field field, final Column column) throws PersistException {

        this.clazz = clazz;
        this.field = field;
        this.family = column.family();
        this.column = column.column().length() > 0 ? column.column() : this.getField().getName();
        this.lookup = column.lookup();
        this.strategy = column.strategy();

        try {
            if (this.isLookupColumn()) {
                this.lookupMethod = clazz.getDeclaredMethod(this.lookup);

                // Check return type and args of lookup method
                final Class<?> retClazz = this.getLookupMethod().getReturnType();

                if (!(retClazz.isArray() && retClazz.getComponentType() == Byte.TYPE))
                    throw new PersistException(clazz.getName() + "." + this.lookup + "()"
                                               + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new PersistException("Missing method " + clazz.getName() + "." + this.lookup + "()");
        }

    }

    public Class getEnclosingClass() {
        return this.clazz;
    }

    private Method getLookupMethod() {
        return lookupMethod;
    }

    public boolean isLookupColumn() {
        return this.lookup.length() > 0;
    }

    public Column.Strategy getStrategy() {
        return this.strategy;
    }

    public boolean isSerializedArray() {
        return this.strategy == Column.Strategy.SERIALIZED_ARRAY;
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

    public byte[] invokeLookupMethod(Object obj) throws IllegalAccessException, InvocationTargetException {
        return (byte[])this.getLookupMethod().invoke(obj);
    }

}
