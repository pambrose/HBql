package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class DefinedAttrib extends ColumnAttrib {

    private final ColumnDescription columnDescription;

    public DefinedAttrib(final ColumnDescription columnDescription) throws HBqlException {
        super(columnDescription.getFamilyName(),
              columnDescription.getColumnName(),
              columnDescription.getAliasName(),
              columnDescription.isMapKeysAsColumns(),
              columnDescription.getFieldType(),
              columnDescription.isArray(),
              null,
              null);

        this.columnDescription = columnDescription;

        if (this.isKeyAttrib() && this.getFamilyName().length() > 0)
            throw new HBqlException("Key value " + this.getObjectQualifiedName() + " cannot have a family name");
    }

    private ColumnDescription getColumnDescription() {
        return this.columnDescription;
    }

    public String getColumnName() {
        return this.getColumnDescription().getColumnName();
    }

    public String getFamilyName() {
        return this.getColumnDescription().getFamilyName();
    }

    public String toString() {
        return this.getAliasName() + " - " + this.getFamilyQualifiedName();
    }

    public boolean isKeyAttrib() {
        return this.getFieldType() == FieldType.KeyType;
    }

    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        final HRecord record = (HRecord)recordObj;
        return record.getCurrentValue(this.getAliasName());
    }

    public void setCurrentValue(final Object newobj, final long ts, final Object val) throws HBqlException {
        final HRecord record = (HRecord)newobj;
        record.setCurrentValue(this.getAliasName(), ts, val);
    }

    public Map<Long, Object> getVersionValueMapValue(final Object recordObj) throws HBqlException {
        final HRecord record = (HRecord)recordObj;
        return record.getVersionedValueMap(this.getAliasName());
    }

    public void setVersionValueMapValue(final Object newobj, final Map<Long, Object> map) {
        final HRecord record = (HRecord)newobj;
        record.setVersionedValueMap(this.getAliasName(), map);
    }

    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return null;
    }

    protected Class getComponentType() {
        return null;
    }

    public String getObjectQualifiedName() {
        return null;
    }

    public String getEnclosingClassName() {
        return null;
    }

    public String[] getNamesForColumn() {
        final List<String> nameList = Lists.newArrayList();
        nameList.add(this.getFamilyQualifiedName());
        if (!this.getAliasName().equals(this.getFamilyQualifiedName()))
            nameList.add(this.getAliasName());
        return nameList.toArray(new String[nameList.size()]);
    }
}
