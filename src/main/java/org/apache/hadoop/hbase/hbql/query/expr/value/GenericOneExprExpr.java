package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public class GenericOneExprExpr {

    private ValueExpr expr = null;

    public GenericOneExprExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    protected ValueExpr getExpr() {
        return this.expr;
    }

    protected void setExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    public boolean isAConstant() throws HBqlException {
        return this.getExpr().isAConstant();
    }

    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
    }
}