package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
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

    @Override
    public T getOptimizedValue() throws HBqlException {
        return (T)this;
    }

    public boolean isAConstant() {
        return false;
    }

    @Override
    public void setExprContext(final ExprContext context) throws HBqlException {
        this.exprContext = context;
        this.getExprContext().addVariable(this);
    }

    protected ExprContext getExprContext() {
        return this.exprContext;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.getFieldType().getExprType();
    }

    @Override
    public String asString() {
        return this.getVariableName();
    }
}
