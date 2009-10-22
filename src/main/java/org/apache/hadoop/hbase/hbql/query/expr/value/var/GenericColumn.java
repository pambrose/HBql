package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;

public abstract class GenericColumn<T extends GenericValue> implements GenericValue {

    private final ColumnAttrib columnAttrib;
    private ExprContext exprContext = null;

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

    public void reset() {
        if (this.getExprContext() != null)
            this.getExprContext().reset();
    }

    public void setExprContext(final ExprContext context) throws HBqlException {
        this.exprContext = context;
        this.getExprContext().addColumnToUsedList(this);
    }

    protected ExprContext getExprContext() {
        return this.exprContext;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.getFieldType().getExprType();
    }

    public String asString() {
        return this.getVariableName();
    }
}
