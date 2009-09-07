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

    public abstract Object getValue(final HPersistable recordObj) throws HPersistException;

    public abstract String getFamilyName();

    public abstract String getFamilyQualifiedName();

    public abstract boolean isMapKeysAsColumns();

    public abstract FieldType getFieldType();

    /*

    public abstract byte[] getValueAsBytes(final Serialization ser,
                                           final HPersistable recordObj) throws HPersistException, IOException;

    public abstract Object getValueFromBytes(final Serialization ser,
                                             final HPersistable recordObj,
                                             final byte[] b) throws IOException, HPersistException;

    public abstract void setValue(final HPersistable newobj, final Object val);

    public abstract void setValue(final Serialization ser,
                                  final HPersistable newobj,
                                  final byte[] b) throws IOException, HPersistException;
                                  */

}
