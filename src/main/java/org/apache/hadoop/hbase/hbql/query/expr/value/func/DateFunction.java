package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateFunction extends Function implements DateValue {

    public enum IntervalType {
        MILLI(1),
        SECOND(1000 * MILLI.getIntervalMillis()),
        MINUTE(60 * SECOND.getIntervalMillis()),
        HOUR(60 * MINUTE.getIntervalMillis()),
        DAY(24 * HOUR.getIntervalMillis()),
        WEEK(7 * DAY.getIntervalMillis()),
        YEAR(52 * WEEK.getIntervalMillis());

        private final long intervalMillis;

        IntervalType(final long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }

        public long getIntervalMillis() {
            return intervalMillis;
        }
    }

    private IntervalType intervalType;

    public DateFunction(final IntervalType intervalType, final GenericValue arg0) {
        super(Type.INTERVAL, arg0);
        this.intervalType = intervalType;
    }

    public DateFunction(final Type functionType, final GenericValue... exprs) {
        super(functionType, exprs);
    }

    private IntervalType getIntervalType() {
        return this.intervalType;
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case DATE: {
                final String datestr = (String)this.getArg(0).getValue(object);
                final String pattern = (String)this.getArg(1).getValue(object);
                final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                try {
                    return formatter.parse(datestr).getTime();
                }
                catch (ParseException e) {
                    throw new HBqlException(e.getMessage());
                }
            }

            case INTERVAL: {
                final Number num = (Number)this.getArg(0).getValue(object);
                final long val = num.longValue();
                return val * this.getIntervalType().getIntervalMillis();
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    protected String getFunctionName() {
        if (this.getFunctionType() == Type.INTERVAL)
            return this.getIntervalType().name();
        else
            return super.getFunctionName();
    }


    public String asString() {
        if (this.getFunctionType() == Type.INTERVAL)
            return this.getIntervalType().name() + "(" + this.getArg(0).asString() + ")";
        else
            return super.asString();
    }
}