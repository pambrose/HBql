package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HRecord;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class VarDescAttrib extends ColumnAttrib {

    private final VarDesc varDesc;

    public VarDescAttrib(final VarDesc varDesc) throws HPersistException {
        super(varDesc.getFieldType(), varDesc.getFamilyName(), varDesc.getColumnName(), null, null, false);
        this.varDesc = varDesc;
    }

    public String getTypeName() {
        return this.varDesc.getTypeName();
    }

    @Override
    public String getVariableName() {
        return this.varDesc.getVariableName();
    }

    @Override
    public String getFamilyQualifiedName() {
        return this.varDesc.getQualifiedName();
    }

    @Override
    public boolean isArray() {
        // TODO This needs to be implemented
        return false;
    }

    @Override
    public boolean isKeyAttrib() {
        return this.getFieldType() == FieldType.KeyType;
    }

    @Override
    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return null;
    }

    @Override
    protected Class getComponentType() {
        return null;
    }

    @Override
    public String getObjectQualifiedName() {
        return null;
    }

    @Override
    public String getEnclosingClassName() {
        return null;
    }

    @Override
    protected void setCurrentValue(final Object newobj, final long timestamp, final Object val) {
        final HRecord record = (HRecord)newobj;
        record.setCurrentValueByVariableName(this.getVariableName(), timestamp, val);
    }

    @Override
    public Object getCurrentValue(final Object recordObj) throws HPersistException {
        final HRecord record = (HRecord)recordObj;
        return record.getCurrentValueByVariableName(this.getVariableName());
    }

    @Override
    protected void setVersionedValueMap(final Object newobj, final Map<Long, Object> map) {
        final HRecord record = (HRecord)newobj;
        record.setVersionedValueMapByVariableName(this.getVariableName(), map);
    }

    @Override
    public Object getVersionedValueMap(final Object recordObj) throws HPersistException {
        final HRecord record = (HRecord)recordObj;
        return record.getVersionedValueMapByVariableName(this.getVariableName());
    }

}
