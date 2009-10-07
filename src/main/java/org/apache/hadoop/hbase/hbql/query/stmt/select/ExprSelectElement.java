package org.apache.hadoop.hbase.hbql.query.stmt.select;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.DelegateColumn;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ExprSelectElement extends ExprContext implements SelectElement {

    private final String asName;

    private ColumnAttrib columnAttrib = null;
    private String familyName = null;
    private String columnName = null;
    private byte[] familyNameBytes = null;
    private byte[] columnNameBytes = null;

    public ExprSelectElement(final GenericValue genericValue, final String asName) {
        super(null, genericValue);
        this.asName = (asName != null) ? asName : genericValue.asString();
    }

    public static ExprSelectElement newExprElement(final GenericValue expr, final String as) {
        return new ExprSelectElement(expr, as);
    }

    public String getAsName() {
        return this.asName;
    }

    public boolean isSimpleColumnReference() {
        return this.getColumnAttrib() != null;
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

    public void validate(final HConnection connection,
                         final HBaseSchema schema,
                         final List<ColumnAttrib> selectAttribList) throws HBqlException {

        this.setSchema(schema);

        selectAttribList.addAll(this.getAttribsUsedInExpr());

        // Look up stuff for simple column references
        if (this.getGenericValue(0) instanceof DelegateColumn) {
            final String name = ((DelegateColumn)this.getGenericValue(0)).getVariableName();
            this.columnAttrib = this.getSchema().getAttribByVariableName(name);
            if (this.getColumnAttrib() != null) {
                this.familyName = this.getColumnAttrib().getFamilyName();
                this.columnName = this.getColumnAttrib().getColumnName();
                this.familyNameBytes = HUtil.ser.getStringAsBytes(this.getColumnAttrib().getFamilyName());
                this.columnNameBytes = HUtil.ser.getStringAsBytes(this.getColumnAttrib().getColumnName());
            }
        }
    }

    private Map getMapKeysAsColumnsValue(final Result result) throws HBqlException {

        final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(this.getFamilyNameBytes());

        Map mapval = Maps.newHashMap();

        for (final byte[] columnBytes : columnMap.keySet()) {

            final String columnName = HUtil.ser.getStringFromBytes(columnBytes);
            final byte[] b = columnMap.get(columnBytes);

            if (columnName.endsWith("]")) {
                final int lbrace = columnName.indexOf("[");
                final String mapcolumn = columnName.substring(0, lbrace);

                if (mapcolumn.equals(this.getColumnName())) {
                    final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                    final Object val = this.getColumnAttrib().getValueFromBytes(null, b);
                    mapval.put(mapKey, val);
                }
            }
        }

        return mapval;
    }

    public void assignCurrentValue(final Object newobj, final Result result) throws HBqlException {

        // See if it is a column reference or a calculation
        if (this.isSimpleColumnReference()) {

            // Bail if it is an annotation history value
            if (!this.getColumnAttrib().isACurrentValue())
                return;

            // If this is a mapKesAsColumns, then we need to build the map from all the applicable columns in the family
            if (this.getColumnAttrib().isMapKeysAsColumns()) {
                final Map mapval = this.getMapKeysAsColumnsValue(result);
                this.getColumnAttrib().setCurrentValue(newobj, 0, mapval);
            }
            else {
                final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
                this.getColumnAttrib().setCurrentValue(newobj, 0, b);
            }
        }
        else {
            // If it is a calculation, then assign according to the AS name
            final String name = this.getAsName();
            final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
            if (attrib != null) {
                final Object elementValue = this.getValue(result);
                attrib.setCurrentValue(newobj, 0, elementValue);
            }
        }
    }

    public Object getValue(final Result result) throws HBqlException {
        return this.evaluate(0, true, false, result);
    }

    public void assignVersionValue(final Object newobj,
                                   final Collection<ColumnAttrib> columnAttribs,
                                   final Result result) throws HBqlException {

        // Bail if it is a calculation or doesn't support version values
        if (!this.isSimpleColumnReference() || !this.getColumnAttrib().isAVersionValue())
            return;

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
        final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(this.getFamilyNameBytes());

        if (columnMap == null)
            return;

        final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(this.getColumnNameBytes());

        if (timeStampMap == null)
            return;

        Map<Long, Object> mapval = this.getColumnAttrib().getVersionValueMapValue(newobj);

        if (mapval == null) {
            mapval = new TreeMap();
            this.getColumnAttrib().setVersionValueMapValue(newobj, mapval);
        }

        for (final Long timestamp : timeStampMap.keySet()) {
            final byte[] b = timeStampMap.get(timestamp);
            final Object val = this.getColumnAttrib().getValueFromBytes(newobj, b);
            mapval.put(timestamp, val);
        }
    }

    public String asString() {
        return this.getGenericValue(0).asString();
    }

    public boolean useHBaseResult() {
        return true;
    }
}
