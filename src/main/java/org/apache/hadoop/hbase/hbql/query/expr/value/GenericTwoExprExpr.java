package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public class GenericTwoExprExpr<T extends ValueExpr> {

    private T expr1 = null, expr2 = null;
    private Class<T> expr1Type = null, expr2Type = null;

    public GenericTwoExprExpr(final T expr1, final T expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected T getExpr1() {
        return this.expr1;
    }

    protected void setExpr1(final T expr1) {
        this.expr1 = expr1;
    }

    protected T getExpr2() {
        return this.expr2;
    }

    protected void setExpr2(final T expr2) {
        this.expr2 = expr2;
    }

    public Class<T> getExpr1Type() {
        return expr1Type;
    }

    public void setExpr1Type(final Class<T> expr1Type) {
        this.expr1Type = expr1Type;
    }

    public Class<T> getExpr2Type() {
        return expr2Type;
    }

    public void setExpr2Type(final Class<T> expr2Type) {
        this.expr2Type = expr2Type;
    }

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        if (this.getExpr2() != null)
            retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    public boolean isAConstant() {
        if (this.getExpr2() == null)
            return this.getExpr1().isAConstant();
        else
            return (this.getExpr1().isAConstant() && this.getExpr2().isAConstant());
    }

    public void setContext(final ExprTree context) {
        this.getExpr1().setContext(context);
        this.getExpr2().setContext(context);
    }
}
