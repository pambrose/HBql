package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.StringValue;
import com.imap4j.hbase.hbql.expr.ValueExpr;

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
    public String getValue(final AttribContext context) throws HPersistException {

        if (vals.size() == 1)
            return (String)vals.get(0).getValue(context);

        final StringBuffer sbuf = new StringBuffer();
        for (final ValueExpr str : this.vals)
            sbuf.append((String)str.getValue(context));

        return sbuf.toString();
    }
}
