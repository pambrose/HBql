package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class LongLiteral extends GenericLiteral implements NumberValue {

    private final Long value;

    public LongLiteral(final Long value) {
        this.value = value;
    }

    @Override
    public Long getValue(final EvalContext context) {
        return this.value;
    }
}