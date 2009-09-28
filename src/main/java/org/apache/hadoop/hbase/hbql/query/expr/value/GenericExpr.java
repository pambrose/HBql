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

    public ExprArgs getArgs() {
        return this.exprArgs;
    }

    public boolean isAConstant() throws HBqlException {
        return this.getArgs().isAConstant();
    }

    public void setContext(final ExprTree context) {
        this.getArgs().setContext(context);
    }

    public void optimizeArgs() throws HBqlException {
        this.getArgs().optimizeArgs();

    }

    public GenericValue getArg(final int i) {
        return this.getArgs().getArg(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getArgs().setArg(i, val);
    }

}
