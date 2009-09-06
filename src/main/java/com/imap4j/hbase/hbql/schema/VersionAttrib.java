package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HColumnVersionMap;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 5, 2009
 * Time: 10:03:49 PM
 */
public class VersionAttrib implements Serializable {

    private final Field field;
    private final HColumnVersionMap columnVersionMapAnnotation;

    public VersionAttrib(final Field field, final HColumnVersionMap columnVersionMapAnnotation) {
        this.field = field;
        this.columnVersionMapAnnotation = columnVersionMapAnnotation;
    }

    private Field getField() {
        return field;
    }

    private HColumnVersionMap getColumnVersionMapAnnotation() {
        return columnVersionMapAnnotation;
    }

    public String getVariableName() {
        return this.getField().getName();
    }
}
