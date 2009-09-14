package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntegerLiteral extends GenericLiteral implements NumberValue {

    private final Integer value;

    public IntegerLiteral(final Integer value) {
        this.value = value;
    }

    @Override
    public Integer getCurrentValue(final Object object) {
        return this.value;
    }
}