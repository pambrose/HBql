package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;

import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateInStmt extends GenericInStmt implements PredicateExpr {

    private DateValue expr = null;
    private final List<DateValue> valList;

    public DateInStmt(final DateValue expr, final boolean not, final List<DateValue> valList) {
        super(not);
        this.expr = expr;
        this.valList = valList;
    }

    protected DateValue getExpr() {
        return expr;
    }

    private List<DateValue> getValList() {
        return valList;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        for (final DateValue val : this.getValList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new DateLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (!this.optimizeList(context))
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {
        final boolean retval = this.evaluateList(context);
        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.listIsConstant();
    }

    private boolean optimizeList(final EvalContext context) throws HPersistException {

        boolean retval = true;
        final List<DateValue> newvalList = Lists.newArrayList();

        for (final DateValue num : this.getValList()) {
            if (num.optimizeForConstants(context)) {
                newvalList.add(new DateLiteral(num.getValue(context)));
            }
            else {
                newvalList.add(num);
                retval = false;
            }
        }

        // Swap new values to list
        this.getValList().clear();
        this.getValList().addAll(newvalList);

        return retval;
    }

    private boolean evaluateList(final EvalContext context) throws HPersistException {

        final Date attribVal = this.getExpr().getValue(context);
        for (final DateValue obj : this.getValList()) {
            final Date val = obj.getValue(context);
            if (attribVal.equals(val))
                return true;
        }

        return false;
    }

    private boolean listIsConstant() {

        for (final DateValue val : this.getValList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }

}