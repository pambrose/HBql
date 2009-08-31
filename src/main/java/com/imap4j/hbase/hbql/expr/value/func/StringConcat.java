package com.imap4j.hbase.hbql.expr.value.func;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

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

    private List<StringValue> getVals() {
        return this.vals;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (!this.optimizeList(context))
            retval = false;

        return retval;
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {

        if (this.getVals().size() == 1)
            return this.getVals().get(0).getValue(context);

        final StringBuffer sbuf = new StringBuffer();
        for (final StringValue str : this.getVals())
            sbuf.append(str.getValue(context));

        return sbuf.toString();
    }

    private boolean optimizeList(final EvalContext context) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue num : this.getVals()) {
            if (num.optimizeForConstants(context)) {
                newvalList.add(new StringLiteral(num.getValue(context)));
            }
            else {
                newvalList.add(num);
                retval = false;
            }
        }

        // Swap new values to list
        this.getVals().clear();
        this.getVals().addAll(newvalList);

        return retval;

    }
}
