package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.ValueExpr;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericCompare<T extends ValueExpr> implements PredicateExpr {

    public enum OP {
        EQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        NOTEQ
    }

    private final OP op;
    private T expr1 = null, expr2 = null;

    protected GenericCompare(final T expr1, final OP op, final T expr2) {
        this.expr1 = expr1;
        this.op = op;
        this.expr2 = expr2;
    }

    protected OP getOp() {
        return op;
    }

    protected T getExpr1() {
        return this.expr1;
    }

    protected T getExpr2() {
        return this.expr2;
    }

    public void setExpr1(final T expr1) {
        this.expr1 = expr1;
    }

    public void setExpr2(final T expr2) {
        this.expr2 = expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr1().setSchema(schema);
        this.getExpr2().setSchema(schema);
    }

}