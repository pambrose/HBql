package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericFunction {

    public enum Type {
        CONCAT,
        TRIM,
        LOWER,
        UPPER,
        LENGTH,
        CONTAINS,
        REPLACE,
        INDEXOF
    }

    private final Type functionType;
    private final StringValue[] stringExprs;


    protected GenericFunction(final Type functionType, final StringValue... stringExprs) {
        this.functionType = functionType;
        this.stringExprs = stringExprs;
    }

    protected StringValue[] getStringExprs() {
        return stringExprs;
    }


    protected Type getFunctionType() {
        return this.functionType;
    }

    public void setContext(final ExprTree context) {
        for (final StringValue val : this.getStringExprs())
            val.setContext(context);
    }

    // TODO Deal with this
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        return false;
    }

    // TODO Deal with this
    public boolean isAConstant() {
        return false;
    }

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = Lists.newArrayList();
        for (final StringValue val : this.getStringExprs())
            retval.addAll(val.getExprVariables());
        return retval;
    }

}