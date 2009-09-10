package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.ValueExpr;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 1:51:03 PM
 */
public abstract class GenericTernary<T extends ValueExpr> {

    private PredicateExpr pred = null;
    private T expr1 = null, expr2 = null;

    public GenericTernary(final PredicateExpr pred, final T expr1, final T expr2) {
        this.pred = pred;
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected PredicateExpr getPred() {
        return this.pred;
    }

    protected T getExpr1() {
        return this.expr1;
    }

    protected T getExpr2() {
        return this.expr2;
    }

    protected void setPred(final PredicateExpr pred) {
        this.pred = pred;
    }

    protected void setExpr1(final T expr1) {
        this.expr1 = expr1;
    }

    protected void setExpr2(final T expr2) {
        this.expr2 = expr2;
    }

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getPred().getExprVariables();
        retval.addAll(this.getExpr1().getExprVariables());
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    public Object getValue(final Object object) throws HPersistException {

        if (this.getPred().evaluate(object))
            return this.getExpr1().getValue(object);
        else
            return this.getExpr2().getValue(object);
    }

    public boolean isAConstant() {
        return this.getPred().isAConstant() && this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    public void setSchema(final ExprSchema schema) {
        this.getPred().setSchema(schema);
        this.getExpr1().setSchema(schema);
        this.getExpr2().setSchema(schema);
    }

}
