package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.DateValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class DateRangeArgs {

    private long lower = -1;
    private long upper = -1;

    public DateRangeArgs(final DateValue lower, final DateValue upper) {
        try {
            if (lower != null)
                this.lower = lower.getCurrentValue(null);
            if (upper != null)
                this.upper = upper.getCurrentValue(null);
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }
    }

    public boolean isValid() {
        return this.getLower() != -1 && this.getUpper() != -1;
    }

    public long getLower() {
        return lower;
    }

    public long getUpper() {
        return upper;
    }
}