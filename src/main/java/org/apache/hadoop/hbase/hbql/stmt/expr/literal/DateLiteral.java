package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

import java.util.Date;

public class DateLiteral extends GenericLiteral<Long> implements DateValue {

    private static long now = System.currentTimeMillis();

    public DateLiteral(final Date dateval) {
        super(dateval.getTime());
    }

    public DateLiteral(final Long value) {
        super(value);
    }

    public static long getNow() {
        return now;
    }

    public static void resetNow() {
        now = System.currentTimeMillis();
    }

    protected Class<? extends GenericValue> getReturnType() {
        return DateValue.class;
    }

    public String asString() {
        return "\"" + String.format("%1$ta %1$tb %1$td %1$tT %1$tZ %1$tY", new Date(this.getValue(null))) + "\"";
    }
}