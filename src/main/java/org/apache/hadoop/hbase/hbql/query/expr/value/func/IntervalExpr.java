package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 10:03:28 PM
 */
public class IntervalExpr implements DateValue {

    public enum Type {
        MILLI(1),
        SECOND(1000 * MILLI.getIntervalMillis()),
        MINUTE(60 * SECOND.getIntervalMillis()),
        HOUR(60 * MINUTE.getIntervalMillis()),
        DAY(24 * HOUR.getIntervalMillis()),
        WEEK(7 * DAY.getIntervalMillis()),
        YEAR(52 * WEEK.getIntervalMillis());

        private final long intervalMillis;

        Type(final long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }

        public long getIntervalMillis() {
            return intervalMillis;
        }
    }

    private final Type type;
    private NumberValue expr;

    public IntervalExpr(final Type type, final NumberValue expr) {
        this.type = type;
        this.expr = expr;
    }

    private Type getType() {
        return this.type;
    }

    protected NumberValue getExpr() {
        return this.expr;
    }

    protected void setExpr(final NumberValue expr) {
        this.expr = expr;
    }

    @Override
    public ValueExpr getOptimizedValue(final Object object) throws HPersistException {

        this.setExpr((NumberValue)this.getExpr().getOptimizedValue(object));

        return this.isAConstant() ? new DateLiteral(this.getValue(object)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {
        final Number num = this.getExpr().getValue(object);
        final long val = num.longValue();
        return val * this.getType().getIntervalMillis();
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
    }

}