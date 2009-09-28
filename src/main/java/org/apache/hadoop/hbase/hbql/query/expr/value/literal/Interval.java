package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 10:03:28 PM
 */
public class Interval extends GenericExpr implements DateValue {

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

    public Interval(final Type type, final GenericValue arg0) {
        super(null, arg0);
        this.type = type;
    }

    private Type getIntervalType() {
        return this.type;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        this.validateParentClass(NumberValue.class, this.getArg(0).validateTypes(this, false));
        return DateValue.class;
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return this.isAConstant() ? new DateConstant(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {
        final Number num = (Number)this.getArg(0).getValue(object);
        final long val = num.longValue();
        return val * this.getIntervalType().getIntervalMillis();
    }

    @Override
    public String asString() {
        return this.getIntervalType().name() + "(" + this.getArg(0).asString() + ")";
    }

}