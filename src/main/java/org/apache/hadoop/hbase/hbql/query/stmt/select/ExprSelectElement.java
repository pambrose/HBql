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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 1, 2009
 * Time: 8:39:47 PM
 */
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

    public Object getValue(final Result result) throws HBqlException {
        return this.evaluate(0, true, result);
    }

    @Override
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

    @Override
    public void assignCurrentValue(final Object newobj, final Result result) throws HBqlException {

        if (this.isSimpleColumnReference()) {
            if (this.getColumnAttrib().isACurrentValue()) {
                final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
                this.getColumnAttrib().setCurrentValue(newobj, 0, b);
            }
        }
        else {
            final String name = this.getAsName();
            final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
            if (attrib != null) {
                final Object elementValue = this.getValue(result);
                attrib.setCurrentValue(newobj, 0, elementValue);
            }
        }
    }

    @Override
    public void assignVersionValue(final Object newobj,
                                   final Collection<ColumnAttrib> columnAttribs,
                                   final Result result) throws HBqlException {

        // Bail if it is a calculation on a current value
        if (!this.isSimpleColumnReference() || this.getColumnAttrib().isACurrentValue())
            return;

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

        final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(this.getFamilyNameBytes());

        if (columnMap == null)
            return;

        final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(this.getColumnNameBytes());

        if (timeStampMap == null)
            return;

        for (final Long timestamp : timeStampMap.keySet()) {

            Map<Long, Object> mapval = this.getColumnAttrib().getVersionValueMapValue(newobj);

            if (mapval == null) {
                mapval = new TreeMap();
                this.getColumnAttrib().setVersionValueMapValue(newobj, mapval);
            }

            final byte[] b = timeStampMap.get(timestamp);
            final Object val = this.getColumnAttrib().getValueFromBytes(newobj, b);
            mapval.put(timestamp, val);
        }
    }

    @Override
    public String asString() {
        return this.getGenericValue(0).asString();
    }

    @Override
    public boolean useHBaseResult() {
        return true;
    }
}
