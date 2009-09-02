package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.DateValue;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateLiteral extends GenericLiteral implements DateValue {

    private final Date value;

    public DateLiteral(final Date value) {
        this.value = value;
    }

    public DateLiteral(final long val) {
        this.value = new Date(val);
    }

    public DateLiteral() {
        this(System.currentTimeMillis());
    }

    @Override
    public Date getValue(final EvalContext context) {
        return this.value;
    }
}