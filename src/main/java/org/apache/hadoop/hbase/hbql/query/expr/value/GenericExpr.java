package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public class GenericExpr {

    private ExprArgs exprArgs;

    public GenericExpr(final GenericValue... exprs) {
        this.exprArgs = new ExprArgs(exprs);
    }

    public boolean isAConstant() throws HBqlException {
        return this.exprArgs.isAConstant();
    }

    public void setContext(final ExprTree context) {
        this.exprArgs.setContext(context);
    }

    public GenericValue getArg(final int i) {
        return this.exprArgs.getArg(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.exprArgs.setArg(i, val);
    }

}
