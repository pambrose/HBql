package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.util.Date;

public class DateLiteral extends GenericLiteral implements DateValue {

    private static long now = System.currentTimeMillis();

    private final Long dateval;

    public DateLiteral(final Date dateval) {
        this.dateval = dateval.getTime();
    }

    public DateLiteral(final Long val) {
        this.dateval = val;
    }

    public static long getNow() {
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