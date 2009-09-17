package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public class GenericTwoExprExpr<T extends ValueExpr> {

    private T expr1 = null, expr2 = null;

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

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        if (this.getExpr2() != null)
            retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    public void setSchema(final ExprSchema schema) {
        this.getExpr1().setSchema(schema);
        this.getExpr2().setSchema(schema);
    }

}
