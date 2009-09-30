package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 6, 2009
 * Time: 5:19:35 PM
 */
public class DefinedAttrib extends ColumnAttrib {

    private final ColumnDescription columnDescription;

    public DefinedAttrib(final ColumnDescription columnDescription) throws HBqlException {
        super(columnDescription.getFamilyName(),
              columnDescription.getColumnName(),
              columnDescription.getFieldType(),
              null,
              null,
              false);

        this.columnDescription = columnDescription;

        if (this.isKeyAttrib() && this.getFamilyName().length() > 0)
            throw new HBqlException("Key value " + this.getObjectQualifiedName() + " cannot have a family name");
    }

    private ColumnDescription getColumnDescription() {
        return this.columnDescription;
    }

    public String getTypeName() {
        return this.getColumnDescription().getTypeName();
    }

    @Override
    public String getColumnName() {
        return this.getColumnDescription().getVariableName();
    }

    /*
    @Override
    public String getFamilyQualifiedName() {
        return this.getColumnDescription().getFamilyQualifiedName();
    }
    */

    @Override
    public String getFamilyName() {
        return this.getColumnDescription().getFamilyName();
    }

    @Override
    public String toString() {
        return this.getColumnName() + " - " + this.getFamilyQualifiedName();
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
    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        final HRecord record = (HRecord)recordObj;
        return record.getCurrentValue(this.getColumnName());
    }

    @Override
    protected void setCurrentValue(final Object newobj, final long ts, final Object val) throws HBqlException {
        final HRecord record = (HRecord)newobj;
        record.setCurrentValue(this.getColumnName(), ts, val);
    }

    @Override
    public Object getVersionedValueMap(final Object recordObj) throws HBqlException {
        final HRecord record = (HRecord)recordObj;
        return record.getVersionedValueMap(this.getColumnName());
    }

    @Override
    protected void setVersionedValueMap(final Object newobj, final Map<Long, Object> map) {
        final HRecord record = (HRecord)newobj;
        record.setVersionedValueMap(this.getColumnName(), map);
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

}
