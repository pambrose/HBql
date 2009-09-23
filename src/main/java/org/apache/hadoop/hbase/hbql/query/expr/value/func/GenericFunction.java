package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericFunction<T extends ValueExpr> {

    private final FunctionType functionType;
    private final ValueExpr[] valueExprs;

    protected GenericFunction(final FunctionType functionType, final ValueExpr... valueExprs) {
        this.functionType = functionType;
        this.valueExprs = valueExprs;
    }

    protected ValueExpr[] getValueExprs() {
        return valueExprs;
    }

    protected FunctionType getFunctionType() {
        return this.functionType;
    }

    public void setContext(final ExprTree context) {
        for (final ValueExpr val : this.getValueExprs())
            val.setContext(context);
    }

    // TODO Deal with this
    public T getOptimizedValue() throws HPersistException {
        return (T)this;
    }

    // TODO Deal with this
    public boolean isAConstant() {
        return false;
    }

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = Lists.newArrayList();
        for (final ValueExpr val : this.getValueExprs())
            retval.addAll(val.getExprVariables());
        return retval;
    }
}