package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

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

    private ValueExpr formatExpr = null, expr = null;

    public DateExpr(final ValueExpr formatExpr, final ValueExpr expr) {
        this.formatExpr = formatExpr;
        this.expr = expr;
    }

    private ValueExpr getFormatExpr() {
        return this.formatExpr;
    }

    private ValueExpr getExpr() {
        return this.expr;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getFormatExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr().validateType();

        if (!type1.equals(StringValue.class))
            throw new HPersistException("Invalid type " + type1.getName() + " in DateExpr");

        if (!type2.equals(StringValue.class))
            throw new HPersistException("Invalid type " + type2.getName() + " in DateExpr");

        return DateValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.formatExpr = this.getFormatExpr().getOptimizedValue();
        this.expr = this.getExpr().getOptimizedValue();

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {

        final String pattern = ((StringValue)this.getFormatExpr()).getValue(object);
        final String datestr = ((StringValue)this.getExpr()).getValue(object);
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
    public void setContext(final ExprTree context) {
        this.getFormatExpr().setContext(context);
        this.getExpr().setContext(context);
    }
}