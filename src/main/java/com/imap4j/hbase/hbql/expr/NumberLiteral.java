package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.ClassSchema;
import com.imap4j.hbase.hbql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberLiteral implements ValueExpr {

    private final Number value;

    public NumberLiteral(final Number value) {
        this.value = value;
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) {
        return this.value;
    }
}