package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateFunction extends Function implements DateValue {

    public enum ConstantType {
        NOW(true, 0),
        MINDATE(false, 0),
        MAXDATE(false, Long.MAX_VALUE);

        final boolean relative;
        final long value;

        ConstantType(final boolean relative, final long value) {
            this.relative = relative;
            this.value = value;
        }

        public boolean isRelative() {
            return this.relative;
        }

        public long getValue() {
            return this.value;
        }
    }

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
    private DateLiteral dateValue;

    public DateFunction(final Type functionType, final GenericValue... exprs) {
        super(functionType, exprs);
    }

    public DateFunction(final ConstantType type) {
        super(Type.DATELITERAL);
        switch (type) {
            case NOW:
                this.dateValue = new DateLiteral(DateLiteral.getNow());
                break;
            case MINDATE:
                this.dateValue = new DateLiteral(0L);
                break;
            case MAXDATE:
                this.dateValue = new DateLiteral(Long.MAX_VALUE);
                break;
        }
    }

    public DateFunction(final IntervalType intervalType, final GenericValue arg0) {
        super(Type.INTERVAL, arg0);
        this.intervalType = intervalType;
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

            case DATELITERAL: {
                return this.dateValue.getValue(object);
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