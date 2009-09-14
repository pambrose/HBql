package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.NumberLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class Substring implements StringValue {

    private StringValue expr = null;
    private NumberValue begin = null, end = null;

    public Substring(final StringValue expr, final NumberValue begin, final NumberValue end) {
        this.expr = expr;
        this.begin = begin;
        this.end = end;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    private NumberValue getBegin() {
        return this.begin;
    }

    private NumberValue getEnd() {
        return this.end;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        retval.addAll(this.getBegin().getExprVariables());
        retval.addAll(this.getEnd().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new StringLiteral(this.getExpr().getCurrentValue(object));
        else
            retval = false;

        if (this.getBegin().optimizeForConstants(object))
            this.begin = new NumberLiteral(this.getBegin().getCurrentValue(object));
        else
            retval = false;

        if (this.getEnd().optimizeForConstants(object))
            this.end = new NumberLiteral(this.getEnd().getCurrentValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public String getCurrentValue(final Object object) throws HPersistException {
        final String val = this.getExpr().getCurrentValue(object);
        final int begin = this.getBegin().getCurrentValue(object).intValue();
        final int end = this.getEnd().getCurrentValue(object).intValue();
        return val.substring(begin, end);
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.getBegin().isAConstant() && this.getEnd().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr().setSchema(schema);
        this.getBegin().setSchema(schema);
        this.getEnd().setSchema(schema);
    }

}