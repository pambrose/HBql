package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.util.Date;

public class DateLiteral extends GenericLiteral implements DateValue {

    private static long now = System.currentTimeMillis();

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

    private final Long dateval;

    public DateLiteral(final Date dateval) {
        this.dateval = dateval.getTime();
    }

    public DateLiteral(final Long val) {
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

    public Long getValue(final Object object) {
        return this.dateval;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return DateValue.class;
    }

    public String asString() {
        return "\"" + String.format("%1$ta %1$tb %1$td %1$tT %1$tZ %1$tY", new Date(this.dateval)) + "\"";
    }
}