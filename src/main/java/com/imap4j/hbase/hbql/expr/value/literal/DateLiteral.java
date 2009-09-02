package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.EvalContext;
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

    public enum TYPE {
        TODAY(0),
        YESTERDAY(-day),
        TOMORROW(+day);

        final long adjustment;

        TYPE(final long adjustment) {
            this.adjustment = adjustment;
        }

        public long getAdjustment() {
            return adjustment;
        }
    }

    private final Date value;

    public DateLiteral(final Date value) {
        this.value = value;
    }

    public DateLiteral(final long val) {
        this.value = new Date(val);
    }

    public DateLiteral(final TYPE type) {
        this.value = new Date(getNow() + type.getAdjustment());
    }

    private static long getNow() {
        return now;
    }

    public static void resetNow() {
        now = System.currentTimeMillis();
    }

    @Override
    public Date getValue(final EvalContext context) {
        return this.value;
    }
}