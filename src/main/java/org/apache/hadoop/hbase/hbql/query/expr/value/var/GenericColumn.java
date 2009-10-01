package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericColumn<T extends GenericValue> implements GenericValue {

    private final VariableAttrib variableAttrib;

    private ExprContext exprContext = null;

    protected GenericColumn(final VariableAttrib attrib) {
        this.variableAttrib = attrib;
    }

    protected abstract FieldType getFieldType();

    public VariableAttrib getVariableAttrib() {
        return this.variableAttrib;
    }

    public String getColumnName() {
        return this.getVariableAttrib().getColumnName();
    }

    @Override
    public T getOptimizedValue() throws HBqlException {
        return (T)this;
    }

    public boolean isAConstant() throws HBqlException {
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
        return this.getColumnName();
    }
}
