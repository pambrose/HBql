package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
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
public class NullCompare extends GenericNotStmt implements PredicateExpr {

    private StringValue expr = null;

    public NullCompare(final boolean not, final StringValue expr) {
        super(not);
        this.expr = expr;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
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
        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public boolean isAContant() {
        return this.getExpr().isAContant();
    }
}