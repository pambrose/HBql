package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class NullCompare implements PredicateExpr {

    private final boolean not;
    private StringValue expr = null;

    public NullCompare(final boolean not, final StringValue expr) {
        this.not = not;
        this.expr = expr;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    @Override
    public List<String> getAttribNames() {
        return this.getExpr().getAttribNames();
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new StringLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {
        final String val = this.expr.getValue(context);
        final boolean retval = (val == null);
        return (this.not) ? !retval : retval;
    }
}