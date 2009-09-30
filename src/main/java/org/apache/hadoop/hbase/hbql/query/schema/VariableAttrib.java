package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 6:07:31 PM
 */
public abstract class VariableAttrib implements Serializable {

    private final FieldType fieldType;
    private byte[] familyBytes = null;
    private byte[] columnBytes = null;

    protected VariableAttrib(final FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public abstract boolean isArray();

    public abstract String getFamilyName();

    public abstract String getColumnName();

    public abstract Object getCurrentValue(final Object recordObj) throws HBqlException;

    protected abstract void setCurrentValue(final Object newobj, final long timestamp, final Object val) throws HBqlException;

    public abstract Object getVersionedValueMap(final Object recordObj) throws HBqlException;

    protected abstract void setVersionedValueMap(final Object newobj, final Map<Long, Object> map);

    public String getFamilyQualifiedName() {
        if (this.getFamilyName() != null && this.getFamilyName().length() > 0)
            return this.getFamilyName() + ":" + this.getColumnName();
        else
            return this.getColumnName();
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isHBaseAttrib() {
        return true;
    }

    public boolean isKeyAttrib() {
        return false;
    }

    public byte[] getFamilyNameBytes() throws HBqlException {
        if (this.familyBytes != null)
            return this.familyBytes;

        this.familyBytes = HUtil.ser.getStringAsBytes(this.getFamilyName());
        return this.familyBytes;
    }

    public byte[] getColumnNameBytes() throws HBqlException {
        if (this.columnBytes != null)
            return this.columnBytes;

        this.columnBytes = HUtil.ser.getStringAsBytes(this.getColumnName());
        return this.columnBytes;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof VariableAttrib))
            return false;

        final VariableAttrib var = (VariableAttrib)o;

        return var.getColumnName().equals(this.getColumnName())
               && var.getFamilyQualifiedName().equals(this.getFamilyQualifiedName());
    }
}
