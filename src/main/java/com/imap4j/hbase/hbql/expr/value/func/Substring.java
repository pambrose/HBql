package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.IntegerLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class Substring implements StringValue {

    private StringValue expr = null;
    private IntegerValue begin = null, end = null;

    public Substring(final StringValue expr, final IntegerValue begin, final IntegerValue end) {
        this.expr = expr;
        this.begin = begin;
        this.end = end;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    private IntegerValue getBegin() {
        return this.begin;
    }

    private IntegerValue getEnd() {
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
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new StringLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (this.getBegin().optimizeForConstants(context))
            this.begin = new IntegerLiteral(this.getBegin().getValue(context));
        else
            retval = false;

        if (this.getEnd().optimizeForConstants(context))
            this.end = new IntegerLiteral(this.getEnd().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {
        final String val = this.getExpr().getValue(context);
        final int begin = this.getBegin().getValue(context);
        final int end = this.getEnd().getValue(context);
        return val.substring(begin, end);
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.getBegin().isAConstant() && this.getEnd().isAConstant();
    }

}