package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public abstract class VariableAttrib implements Serializable {

    public boolean isKey() {
        return false;
    }

    public abstract String getVariableName();

    public abstract FieldType getFieldType();

    public abstract Object getValue(final HPersistable recordObj) throws HPersistException;

}
