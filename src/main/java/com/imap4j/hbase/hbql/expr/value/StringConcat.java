package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.StringValue;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 6:20:20 PM
 */
public class StringConcat implements StringValue {

    private final List<StringValue> vals;

    public StringConcat(final List<StringValue> vals) {
        this.vals = vals;
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {

        if (vals.size() == 1)
            return this.vals.get(0).getValue(context);

        final StringBuffer sbuf = new StringBuffer();
        for (final StringValue str : this.vals)
            sbuf.append(str.getValue(context));

        return sbuf.toString();
    }
}
