package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HColumn;
import com.imap4j.hbase.hbql.HPersistException;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:17:59 PM
 */
public class CurrentValueAttrib extends ColumnAttrib {


    public CurrentValueAttrib(final Field field) throws HPersistException {
        super(field,
              field.getAnnotation(HColumn.class).family(),
              field.getAnnotation(HColumn.class).column(),
              field.getAnnotation(HColumn.class).getter(),
              field.getAnnotation(HColumn.class).setter(),
              field.getAnnotation(HColumn.class).mapKeysAsColumns());

        try {
            if (this.getGetter().length() > 0) {
                this.getterMethod = this.getEnclosingClass().getDeclaredMethod(this.getGetter());

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HPersistException(this.getEnclosingClass().getName()
                                                + "." + this.getGetter() + "()"
                                                + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method byte[] " + this.getEnclosingClass().getName() + "."
                                        + this.getGetter() + "()");
        }

        try {
            if (this.getSetter().length() > 0) {
                this.setterMethod = this.getEnclosingClass().getDeclaredMethod(this.getSetter(), Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HPersistException(this.getEnclosingClass().getName()
                                                + "." + this.getSetter() + "()" + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Missing method " + this.getEnclosingClass().getName()
                                        + "." + this.getSetter() + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HPersistException("Missing method " + this.getEnclosingClass().getName()
                                        + "." + this.getSetter() + "(byte[] arg)");
        }

        this.verify();
    }

    private void verify() throws HPersistException {

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

    public boolean isKey() {
        return this.getColumnAnno().key();
    }

}
