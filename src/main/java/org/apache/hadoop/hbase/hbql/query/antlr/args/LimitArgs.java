package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class LimitArgs {

    private long value = -1;

    public LimitArgs(final NumberValue val) {
        try {
            if (val != null)
                this.value = val.getValue(null).longValue();
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }
    }

    public long getValue() {
        return this.value;
    }
}