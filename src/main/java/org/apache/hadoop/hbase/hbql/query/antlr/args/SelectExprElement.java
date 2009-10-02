package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.DelegateColumn;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

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
public class SelectExprElement extends ExprContext implements SelectElement {

    private final String asName;

    private ColumnAttrib columnAttrib = null;
    private String familyName = null;
    private String columnName = null;
    private byte[] familyNameBytes = null;
    private byte[] columnNameBytes = null;

    private Object evaluationValue = null;

    public SelectExprElement(final GenericValue genericValue, final String asName) {
        super(null, genericValue);
        this.asName = (asName != null) ? asName : genericValue.asString();
    }

    public static SelectExprElement newExprElement(final GenericValue expr, final String as) {
        return new SelectExprElement(expr, as);
    }

    public String getAsName() {
        return this.asName;
    }

    public boolean isSimpleColumnReference() {
        return this.getColumnAttrib() != null;
    }

    public ColumnAttrib getColumnAttrib() {
        return columnAttrib;
    }

    public String getFamilyName() {
        return familyName;
    }

    public String getColumnName() {
        return columnName;
    }

    public byte[] getFamilyNameBytes() {
        return familyNameBytes;
    }

    public byte[] getColumnNameBytes() {
        return columnNameBytes;
    }

    public Object getEvaluationValue() {
        return evaluationValue;
    }

    public void evaluate(final Result result) throws HBqlException {

        this.validateTypes(true);
        this.optimize();

        // Set it once per evaluation
        DateLiteral.resetNow();

        this.evaluationValue = this.getGenericValue(0).getValue(result);
    }

    @Override
    public void validate(final HBaseSchema schema,
                         final List<ColumnAttrib> selectAttribList) throws HBqlException {

        this.setSchema(schema);

        selectAttribList.addAll(this.getFamilyQualifiedColumnAttribList());

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
            final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
            this.getColumnAttrib().setCurrentValue(newobj, 0, b);
        }
        else {
            this.evaluate(result);
            final String name = this.getAsName();
            final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
            if (attrib != null)
                attrib.setCurrentValue(newobj, 0, this.getEvaluationValue());
        }

    }

    @Override
    public void assignVersionValue(final Object newobj, final Result result) throws HBqlException {

        // Bail if it is a calculation
        if (!this.isSimpleColumnReference())
            return;

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
        final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(this.getFamilyNameBytes());

        final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(this.getColumnNameBytes());

        // Ignore data if no version map exists for the column
        if (this.getColumnAttrib() == null)
            return;

        // Ignore if not in select list
        if (!columnAttribs.contains(this.getColumnAttrib()))
            continue;

        for (final Long timestamp : timeStampMap.keySet()) {

            Map<Long, Object> mapval = (Map<Long, Object>)this.getColumnAttrib().getMapValue(newobj);

            if (mapval == null) {
                mapval = new TreeMap();
                this.getColumnAttrib().setMapValue(newobj, mapval);
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
