package org.apache.expreval.expr.function;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class DateFunction extends Function implements DateValue {

    public enum ConstantType {
        NOW(0),
        MINDATE(0),
        MAXDATE(Long.MAX_VALUE);

        final long value;

        ConstantType(final long value) {
            this.value = value;
        }

        public long getValue() {
            return this.value;
        }

        public static Function getFunction(final String functionName) {

            try {
                final ConstantType type = ConstantType.valueOf(functionName.toUpperCase());
                return new DateFunction(type);
            }
            catch (IllegalArgumentException e) {
                return null;
            }
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

        public static Function getFunction(final String functionName, final List<GenericValue> exprList) {

            try {
                final IntervalType type = IntervalType.valueOf(functionName.toUpperCase());
                return new DateFunction(type, exprList);
            }
            catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    private ConstantType constantType;
    private IntervalType intervalType;
    private DateLiteral dateValue;

    public DateFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public DateFunction(final ConstantType constantType) {
        super(FunctionType.DATECONSTANT, null);
        this.constantType = constantType;
        switch (this.getConstantType()) {
            case NOW:
                this.dateValue = new DateLiteral(DateLiteral.getNow());
                break;
            case MINDATE:
            case MAXDATE:
                this.dateValue = new DateLiteral(constantType.getValue());
                break;
        }
    }

    public DateFunction(final IntervalType intervalType, final List<GenericValue> exprs) {
        super(FunctionType.DATEINTERVAL, exprs);
        this.intervalType = intervalType;
    }

    private ConstantType getConstantType() {
        return this.constantType;
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

            case DATEINTERVAL: {
                final Number num = (Number)this.getArg(0).getValue(object);
                final long val = num.longValue();
                return val * this.getIntervalType().getIntervalMillis();
            }

            case DATECONSTANT: {
                return this.dateValue.getValue(object);
            }

            case RANDOMDATE: {
                return Math.abs(Function.randomVal.nextLong());
            }

            case LONGTODATE: {
                final Number num = (Number)this.getArg(0).getValue(object);
                final long val = num.longValue();
                this.dateValue = new DateLiteral(val);
                return this.dateValue.getValue(object);
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    protected String getFunctionName() {
        if (this.isIntervalDate())
            return this.getIntervalType().name();
        else if (this.isConstantDate())
            return this.getConstantType().name();
        else
            return super.getFunctionName();
    }


    public String asString() {
        if (this.isIntervalDate())
            return this.getIntervalType().name() + "(" + this.getArg(0).asString() + ")";
        else if (this.isConstantDate())
            return this.getConstantType().name() + "()";
        else
            return super.asString();
    }
}