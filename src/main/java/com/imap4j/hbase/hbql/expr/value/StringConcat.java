package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 6:20:20 PM
 */
public class StringConcat implements ValueExpr {

    private final List<ValueExpr> vals;

    public StringConcat(final List<ValueExpr> vals) {
        this.vals = vals;
    }

    @Override
    public Object getValue(final AttribContext context) throws HPersistException {

        if (vals.size() == 1)
            return vals.get(0).getValue(context);

        final StringBuffer sbuf = new StringBuffer();
        for (final ValueExpr str : this.vals)
            sbuf.append(str.getValue(context));

        return sbuf.toString();
    }
}
