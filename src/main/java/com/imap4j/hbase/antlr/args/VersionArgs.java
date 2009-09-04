package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class VersionArgs {

    private Integer value;

    public VersionArgs(final IntegerValue val) {
        try {
            if (this.value != null)
                this.value = val.getValue(null);
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }
    }

    public Integer getValue() {
        return value;
    }
}