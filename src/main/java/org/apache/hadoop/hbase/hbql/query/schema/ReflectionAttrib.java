package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HPersistException;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class ReflectionAttrib extends FieldAttrib {

    public ReflectionAttrib(final Field field) throws HPersistException {
        super(field, FieldType.getFieldType(field.getType()), null, null, null, null, false);

        this.defineAccessors();
    }

    public boolean isHBaseAttrib() {
        return false;
    }
}