package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberLiteral extends GenericLiteral implements NumberValue {

    private final Number value;

    public NumberLiteral(final Number value) {
        this.value = value;
    }

    @Override
    public Number getValue(final EvalContext context) {
        return this.value;
    }
}