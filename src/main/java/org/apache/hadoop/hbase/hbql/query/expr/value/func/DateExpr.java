package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

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
            this.formatExpr = new StringLiteral(this.getFormatExpr().getCurrentValue(object));
        else
            retval = false;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new StringLiteral(this.getExpr().getCurrentValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Long getCurrentValue(final Object object) throws HPersistException {

        final String pattern = this.getFormatExpr().getCurrentValue(object);
        final String datestr = this.getExpr().getCurrentValue(object);
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
    public void setSchema(final Schema schema) {
        this.getFormatExpr().setSchema(schema);
        this.getExpr().setSchema(schema);
    }

}