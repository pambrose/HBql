package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.IntegerLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntegerBetweenStmt extends GenericBetweenStmt implements PredicateExpr {

    private IntegerValue expr = null;
    private IntegerValue lower = null, upper = null;

    public IntegerBetweenStmt(final IntegerValue expr, final boolean not, final IntegerValue lower, final IntegerValue upper) {
        super(not);
        this.expr = expr;
        this.lower = lower;
        this.upper = upper;
    }

    protected IntegerValue getExpr() {
        return this.expr;
    }

    protected IntegerValue getLower() {
        return this.lower;
    }

    protected IntegerValue getUpper() {
        return this.upper;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new IntegerLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(context))
            this.lower = new IntegerLiteral(this.getLower().getValue(context));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(context))
            this.upper = new IntegerLiteral(this.getUpper().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final EvalContext context) throws HPersistException {

        final int numval = this.getExpr().getValue(context).intValue();
        final boolean retval = numval >= this.getLower().getValue(context).intValue()
                               && numval <= this.getUpper().getValue(context).intValue();

        return (this.isNot()) ? !retval : retval;
    }

}