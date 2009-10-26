package org.apache.hadoop.hbase.hbql.stmt.select;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.var.DelegateColumn;
import org.apache.hadoop.hbase.hbql.stmt.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.stmt.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;
import org.apache.hadoop.hbase.hbql.stmt.util.HUtil;
import org.apache.hadoop.hbase.hbql.stmt.util.Maps;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

public class ExprElement extends ExprContext implements SelectElement {

    private String asName;

    private ColumnAttrib columnAttrib = null;
    private String familyName = null;
    private String columnName = null;
    private byte[] familyNameBytes = null;
    private byte[] columnNameBytes = null;

    private ExprElement(final GenericValue genericValue, final String asName) {
        super(null, genericValue);
        this.asName = asName;
    }

    public static ExprElement newExprElement(final GenericValue expr, final String as) {
        return new ExprElement(expr, as);
    }

    public String getAsName() {
        return this.asName;
    }

    public boolean hasAsName() {
        return this.getAsName() != null && this.getAsName().length() > 0;
    }

    public boolean isSimpleColumnReference() {
        return this.getGenericValue(0) instanceof DelegateColumn;
    }

    public boolean isAConstant() {
        return this.getGenericValue(0).isAConstant();
    }

    public boolean hasAColumnReference() {
        return this.getGenericValue(0).hasAColumnReference();
    }

    public ColumnAttrib getColumnAttrib() {
        return this.columnAttrib;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public byte[] getFamilyNameBytes() {
        return this.familyNameBytes;
    }

    public byte[] getColumnNameBytes() {
        return this.columnNameBytes;
    }

    public void validate(final HBaseSchema schema, final HConnection connection) throws HBqlException {

        this.setSchema(schema);

        // Look up stuff for simple column references
        // TODO this needs to be down for expressions with col refs
        if (this.isSimpleColumnReference()) {
            final String name = ((DelegateColumn)this.getGenericValue(0)).getVariableName();
            this.columnAttrib = this.getSchema().getAttribByVariableName(name);

            if (this.getColumnAttrib() != null) {
                this.familyName = this.getColumnAttrib().getFamilyName();
                this.columnName = this.getColumnAttrib().getColumnName();
            }
            else {
                if (!name.contains(":"))
                    throw new HBqlException("Invalid select value: " + name);
                final String[] strs = name.split(":");
                this.familyName = strs[0];
                this.columnName = strs[1];
                final Collection<String> families = this.getSchema().getSchemaFamilyNames(connection);
                if (!families.contains(this.getFamilyName()))
                    throw new HBqlException("Unknown family name: " + this.getFamilyName());
            }

            this.familyNameBytes = HUtil.getSerialization().getStringAsBytes(this.getFamilyName());
            this.columnNameBytes = HUtil.getSerialization().getStringAsBytes(this.getColumnName());
        }
    }

    public void assignAsNamesForExpressions(final SelectStatement selectStatement) {

        if (!this.isSimpleColumnReference() && !this.hasAsName()) {
            while (true) {
                // Assign a name that is not in use
                final String newAsName = selectStatement.getNextExpressionName();
                if (!selectStatement.hasAsName(newAsName)) {
                    this.asName = newAsName;
                    break;
                }
            }
        }
    }

    private Map<String, Object> getMapKeysAsColumnsValue(final Result result) throws HBqlException {

        final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(this.getFamilyNameBytes());

        final Map<String, Object> mapval = Maps.newHashMap();

        for (final byte[] columnBytes : columnMap.keySet()) {

            final String columnName = HUtil.getSerialization().getStringFromBytes(columnBytes);

            if (columnName.endsWith("]")) {
                final int lbrace = columnName.indexOf("[");
                final String mapcolumn = columnName.substring(0, lbrace);

                if (mapcolumn.equals(this.getColumnName())) {
                    final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);

                    final byte[] b = columnMap.get(columnBytes);
                    final Object val = this.getColumnAttrib().getValueFromBytes(null, b);

                    mapval.put(mapKey, val);
                }
            }
        }
        return mapval;
    }

    private String getSelectName() {
        return this.hasAsName() ? this.getAsName() : this.getFamilyName() + ":" + this.getColumnName();
    }

    private byte[] getResultCurrentValue(final Result result) {

        final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(this.getFamilyNameBytes());

        // ColumnMap should not be null at this point, but check just in case
        if (columnMap == null)
            return null;
        else
            return columnMap.get(this.getColumnNameBytes());
    }

    private void assignCalculation(final Object obj, final Result result) throws HBqlException {
        // If it is a calculation, then assign according to the AS name
        final String name = this.getAsName();
        final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);

        final Object elementValue = this.getValue(result);

        if (attrib == null) {
            // Find value in results and assign the byte[] value to HRecord, but bail on Annotated object
            if (!(obj instanceof HRecord))
                return;

            ((HRecordImpl)obj).setCurrentValue(name, 0, elementValue, false);
        }
        else {
            attrib.setCurrentValue(obj, 0, elementValue);
        }
    }

    public void assignValues(final Object obj,
                             final int maxVerions,
                             final Result result) throws HBqlException {

        // If it is a calculation, take care of it and then bail since calculations have no history
        if (!this.isSimpleColumnReference()) {
            this.assignCalculation(obj, result);
            return;
        }

        final HBaseSchema schema = (HBaseSchema)this.getSchema();

        // Column reference is not known to schema, so just assign byte[] value
        if (this.getColumnAttrib() == null) {
            final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(this.getFamilyName());
            if (familyDefaultAttrib != null) {
                final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
                familyDefaultAttrib.setFamilyDefaultCurrentValue(obj, this.getSelectName(), b);
            }
        }
        else {
            if (this.getColumnAttrib().isACurrentValue()) {
                // If this is a mapKeysAsColumns, then we need to build the map from all the related columns in the family
                if (this.getColumnAttrib().isMapKeysAsColumnsAttrib()) {
                    final Map<String, Object> kacMap = this.getMapKeysAsColumnsValue(result);
                    for (final String mapKey : kacMap.keySet())
                        this.getColumnAttrib().setKeysAsColumnsValue(obj, mapKey, kacMap.get(mapKey));
                }
                else {
                    final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
                    this.getColumnAttrib().setCurrentValue(obj, 0, b);
                }
            }
        }

        // Now assign versions if they were requested. Do not process if it doesn't support version values
        if (maxVerions > 1) {

            // Bail if a known column is not a version attrib
            if (this.getColumnAttrib() != null && !this.getColumnAttrib().isAVersionValue())
                return;

            final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(this.getFamilyNameBytes());

            if (columnMap == null)
                return;

            final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(this.getColumnNameBytes());

            if (this.getColumnAttrib() == null) {
                final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(this.getFamilyName());
                if (familyDefaultAttrib != null)
                    familyDefaultAttrib.setFamilyDefaultVersionMap(obj, this.getSelectName(), timeStampMap);
            }
            else {
                final Map<Long, Object> mapVal = this.getColumnAttrib().getVersionMap(obj);
                for (final Long timestamp : timeStampMap.keySet()) {
                    final byte[] b = timeStampMap.get(timestamp);
                    final Object val = this.getColumnAttrib().getValueFromBytes(obj, b);
                    mapVal.put(timestamp, val);
                }
            }
        }
    }

    public Object getValue(final Result result) throws HBqlException {
        try {
            return this.evaluate(0, true, false, result);
        }
        catch (ResultMissingColumnException e) {
            return null;
        }
    }

    public String asString() {
        return this.getGenericValue(0).asString();
    }

    public boolean useHBaseResult() {
        return true;
    }
}
