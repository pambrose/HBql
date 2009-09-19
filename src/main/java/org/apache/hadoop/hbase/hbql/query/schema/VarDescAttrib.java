package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.client.HRecord;

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

    private VarDesc getVarDesc() {
        return this.varDesc;
    }

    public String getTypeName() {
        return this.getVarDesc().getTypeName();
    }

    @Override
    public String getVariableName() {
        return this.getVarDesc().getVariableName();
    }

    @Override
    public String getFamilyQualifiedName() {
        return this.getVarDesc().getFamilyQualifiedName();
    }

    @Override
    public String toString() {
        return this.getVariableName() + " - " + this.getFamilyQualifiedName();
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
    protected void setCurrentValue(final Object newobj, final long ts, final Object val) throws HPersistException {
        final HRecord record = (HRecord)newobj;
        record.setCurrentValue(this.getVariableName(), ts, val);
    }

    @Override
    public Object getCurrentValue(final Object recordObj) throws HPersistException {
        final HRecord record = (HRecord)recordObj;
        return record.getCurrentValue(this.getVariableName());
    }

    @Override
    protected void setVersionedValueMap(final Object newobj, final Map<Long, Object> map) {
        final HRecord record = (HRecord)newobj;
        record.setVersionedValueMap(this.getVariableName(), map);
    }

    @Override
    public Object getVersionedValueMap(final Object recordObj) throws HPersistException {
        final HRecord record = (HRecord)recordObj;
        return record.getVersionedValueMap(this.getVariableName());
    }

}
