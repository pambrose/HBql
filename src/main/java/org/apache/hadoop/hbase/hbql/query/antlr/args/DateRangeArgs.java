package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;

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