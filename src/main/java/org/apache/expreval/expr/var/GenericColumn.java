package org.apache.expreval.expr.var;

import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.MapValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.schema.FieldType;

public abstract class GenericColumn<T extends GenericValue> implements GenericValue {

    private final ColumnAttrib columnAttrib;
    private MultipleExpressionContext expressionContext = null;

    protected GenericColumn(final ColumnAttrib attrib) {
        this.columnAttrib = attrib;
    }

    protected FieldType getFieldType() {
        return this.getColumnAttrib().getFieldType();
    }

    public ColumnAttrib getColumnAttrib() {
        return this.columnAttrib;
    }

    public String getVariableName() {
        return this.getColumnAttrib().getFamilyQualifiedName();
    }

    public T getOptimizedValue() throws HBqlException {
        return (T)this;
    }

    public boolean isAConstant() {
        return false;
    }

    public boolean isDefaultKeyword() {
        return false;
    }

    public boolean hasAColumnReference() {
        return true;
    }

    public void reset() {
        if (this.getExprContext() != null)
            this.getExprContext().reset();
    }

    public void setExpressionContext(final MultipleExpressionContext context) throws HBqlException {
        this.expressionContext = context;
        this.getExprContext().addColumnToUsedList(this);
    }

    protected MultipleExpressionContext getExprContext() {
        return this.expressionContext;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        if (this.getColumnAttrib().isMapKeysAsColumnsAttrib())
            return MapValue.class;
        else if (this.getColumnAttrib().isAnArray())
            return ObjectValue.class;
        else
            return this.getFieldType().getExprType();
    }

    public String asString() {
        return this.getVariableName();
    }
}
