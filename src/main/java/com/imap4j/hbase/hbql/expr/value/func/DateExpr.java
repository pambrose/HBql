package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateExpr implements DateValue {

    private StringValue formatExpr = null, expr = null;

    public DateExpr(final StringValue formatExpr, final StringValue expr) {
        this.formatExpr = formatExpr;
        this.expr = expr;
    }

    protected StringValue getFormatExpr() {
        return this.formatExpr;
    }

    protected StringValue getExpr() {
        return this.expr;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getFormatExpr().optimizeForConstants(object))
            this.formatExpr = new StringLiteral(this.getFormatExpr().getValue(object));
        else
            retval = false;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new StringLiteral(this.getExpr().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {

        final String pattern = this.getFormatExpr().getValue(object);
        final String datestr = this.getExpr().getValue(object);
        final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

        try {
            return formatter.parse(datestr).getTime();
        }
        catch (ParseException e) {
            throw new HPersistException(e.getMessage());
        }
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getFormatExpr().getExprVariables();
        retval.addAll(this.getExpr().getExprVariables());
        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getFormatExpr().isAConstant() && this.getExpr().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getFormatExpr().setSchema(schema);
        this.getExpr().setSchema(schema);
    }

}