package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class VersionArgs {

    private int value = -1;

    public VersionArgs(final NumberValue val) {
        try {
            if (val != null)
                this.value = val.getValue(null).intValue();
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }
    }

    public boolean isValid() {
        return this.getValue() != -1;
    }

    public int getValue() {
        return this.value;
    }
}