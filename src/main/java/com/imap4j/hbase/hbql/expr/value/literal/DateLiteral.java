package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.node.DateValue;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateLiteral extends GenericLiteral implements DateValue {

    private static long now = System.currentTimeMillis();

    private final static long day = 1000 * 60 * 60 * 24;

    public enum Type {
        NOW(true, 0),
        MINDATE(false, 0),
        MAXDATE(false, Long.MAX_VALUE);

        final boolean adjusted;
        final long value;

        Type(final boolean adjusted, final long value) {
            this.adjusted = adjusted;
            this.value = value;
        }

        public boolean isAdjustment() {
            return this.adjusted;
        }

        public long getValue() {
            return this.value;
        }
    }

    private final long dateval;

    public DateLiteral(final Date dateval) {
        this.dateval = dateval.getTime();
    }

    public DateLiteral(final long val) {
        this.dateval = val;
    }

    public DateLiteral(final Type type) {
        if (type.isAdjustment())
            this.dateval = getNow() + type.getValue();
        else
            this.dateval = type.getValue();
    }

    private static long getNow() {
        return now;
    }

    public static void resetNow() {
        now = System.currentTimeMillis();
    }

    @Override
    public Long getValue(final Object object) {
        return this.dateval;
    }
}