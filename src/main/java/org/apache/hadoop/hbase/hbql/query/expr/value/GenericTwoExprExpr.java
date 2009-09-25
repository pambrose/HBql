package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public class GenericTwoExprExpr {

    private ValueExpr expr1 = null, expr2 = null;

    public GenericTwoExprExpr(final ValueExpr expr1, final ValueExpr expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected ValueExpr getExpr1() {
        return this.expr1;
    }

    protected void setExpr1(final ValueExpr expr1) {
        this.expr1 = expr1;
    }

    protected ValueExpr getExpr2() {
        return this.expr2;
    }

    protected void setExpr2(final ValueExpr expr2) {
        this.expr2 = expr2;
    }

    public boolean isAConstant() {
        if (this.getExpr2() == null)
            return this.getExpr1().isAConstant();
        else
            return (this.getExpr1().isAConstant() && this.getExpr2().isAConstant());
    }

    public void setContext(final ExprTree context) {
        this.getExpr1().setContext(context);
        if (this.getExpr2() != null)
            this.getExpr2().setContext(context);
    }
}
