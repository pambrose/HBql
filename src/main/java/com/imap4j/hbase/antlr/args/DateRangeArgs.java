package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.node.DateValue;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class DateRangeArgs {

    private Date lower = null;
    private Date upper = null;

    public DateRangeArgs(final DateValue lower, final DateValue upper) {
        try {
            if (this.lower != null)
                this.lower = lower.getValue(null);

            if (this.upper != null)
                this.upper = upper.getValue(null);
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }
    }

    public Date getLower() {
        return lower;
    }

    public Date getUpper() {
        return upper;
    }
}