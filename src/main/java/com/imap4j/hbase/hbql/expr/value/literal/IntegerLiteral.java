package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntegerLiteral extends GenericLiteral implements IntegerValue {

    private final Number value;

    public IntegerLiteral(final Number value) {
        this.value = value;
    }

    @Override
    public Number getValue(final EvalContext context) {
        return this.value;
    }
}