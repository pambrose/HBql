package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class StringTernary extends GenericTernary implements StringValue {

    private StringValue expr1 = null, expr2 = null;

    public StringTernary(final PredicateExpr pred, final StringValue expr1, final StringValue expr2) {
        super(pred);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected StringValue getExpr1() {
        return this.expr1;
    }

    protected StringValue getExpr2() {
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
            this.expr1 = new StringLiteral(this.getExpr1().getValue(object));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.expr2 = new StringLiteral(this.getExpr2().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {

        if (this.getPred().evaluate(object))
            return this.getExpr1().getValue(object);
        else
            return this.getExpr2().getValue(object);
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