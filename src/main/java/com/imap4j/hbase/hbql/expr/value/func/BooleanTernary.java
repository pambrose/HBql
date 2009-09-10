package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.BooleanValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class BooleanTernary extends GenericTernary implements BooleanValue {

    private PredicateExpr expr1 = null, expr2 = null;

    public BooleanTernary(final PredicateExpr pred, final PredicateExpr expr1, final PredicateExpr expr2) {
        super(pred);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected PredicateExpr getExpr1() {
        return this.expr1;
    }

    protected PredicateExpr getExpr2() {
        return this.expr2;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getPred().optimizeForConstants(object))
            this.pred = new BooleanLiteral(this.getPred().evaluate(object));
        else
            retval = false;

        if (this.getExpr1().optimizeForConstants(object))
            this.expr1 = new BooleanLiteral(this.getExpr1().evaluate(object));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.expr2 = new BooleanLiteral(this.getExpr2().evaluate(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        if (this.getPred().evaluate(object))
            return this.getExpr1().evaluate(object);
        else
            return this.getExpr2().evaluate(object);
    }

    @Override
    public boolean isAConstant() {
        return this.getPred().isAConstant() && this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getPred().setSchema(schema);
        this.getExpr1().setSchema(schema);
        this.getExpr2().setSchema(schema);
    }
}