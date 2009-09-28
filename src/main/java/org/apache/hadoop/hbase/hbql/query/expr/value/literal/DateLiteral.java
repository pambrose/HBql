package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateLiteral implements DateValue {

    private GenericValue formatExpr = null, valueExpr = null;

    public DateLiteral(final GenericValue formatExpr, final GenericValue valueExpr) {
        this.formatExpr = formatExpr;
        this.valueExpr = valueExpr;
    }

    private GenericValue getFormatExpr() {
        return this.formatExpr;
    }

    private GenericValue getValueExpr() {
        return this.valueExpr;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        HUtil.validateParentClass(this,
                                  StringValue.class,
                                  this.getFormatExpr().validateTypes(this, false),
                                  this.getValueExpr().validateTypes(this, false));
        return DateValue.class;
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        this.formatExpr = this.getFormatExpr().getOptimizedValue();
        this.valueExpr = this.getValueExpr().getOptimizedValue();

        return this.isAConstant() ? new DateConstant(this.getValue(null)) : this;
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
    public boolean isAConstant() throws HBqlException {
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