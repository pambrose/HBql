package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

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

    private ValueExpr formatExpr = null, valueExpr = null;

    public DateExpr(final ValueExpr formatExpr, final ValueExpr valueExpr) {
        this.formatExpr = formatExpr;
        this.valueExpr = valueExpr;
    }

    private ValueExpr getFormatExpr() {
        return this.formatExpr;
    }

    private ValueExpr getValueExpr() {
        return this.valueExpr;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getFormatExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getValueExpr().validateType();

        if (!HUtil.isParentClass(StringValue.class, type1))
            throw new HPersistException("Invalid type " + type1.getName() + " in DateExpr.validateType()");

        if (!HUtil.isParentClass(StringValue.class, type2))
            throw new HPersistException("Invalid type " + type2.getName() + " in DateExpr.validateType()");

        return DateValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.formatExpr = this.getFormatExpr().getOptimizedValue();
        this.valueExpr = this.getValueExpr().getOptimizedValue();

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {

        final String pattern = (String)this.getFormatExpr().getValue(object);
        final String datestr = (String)this.getValueExpr().getValue(object);
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
        retval.addAll(this.getValueExpr().getExprVariables());
        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getFormatExpr().isAConstant() && this.getValueExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getFormatExpr().setContext(context);
        this.getValueExpr().setContext(context);
    }

    @Override
    public void setParam(final String param, final Object val) throws HPersistException {
        this.getFormatExpr().setParam(param, val);
        this.getValueExpr().setParam(param, val);
    }
}