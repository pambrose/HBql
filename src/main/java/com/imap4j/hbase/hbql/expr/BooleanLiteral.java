package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanLiteral implements ValueExpr {

    private final Boolean value;

    public BooleanLiteral(final String text) {
        this.value = text.equalsIgnoreCase("true");
    }

    @Override
    public Object getValue(final ClassSchema classSchema, final HPersistable recordObj) {
        return this.value;
    }
}