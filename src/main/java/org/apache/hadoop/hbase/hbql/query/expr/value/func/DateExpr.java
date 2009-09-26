package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

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
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr) throws TypeException {
        HUtil.validateParentClass(this,
                                  StringValue.class,
                                  this.getFormatExpr().validateTypes(this),
                                  this.getValueExpr().validateTypes(this));
        return DateValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {

        this.formatExpr = this.getFormatExpr().getOptimizedValue();
        this.valueExpr = this.getValueExpr().getOptimizedValue();

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final String pattern = (String)this.getFormatExpr().getValue(object);
        final String datestr = (String)this.getValueExpr().getValue(object);
        final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

        try {
            return formatter.parse(datestr).getTime();
        }
        catch (ParseException e) {
            throw new HBqlException(e.getMessage());
        }
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
    public String asString() {
        return "DATE(" + this.getFormatExpr().asString() + ", " + this.getValueExpr().asString() + ")";
    }

}