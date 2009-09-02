package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
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
 * Time: 6:58:31 PM
 */
public class StringInStmt extends GenericInStmt implements PredicateExpr {

    private StringValue expr = null;
    private final List<StringValue> valList;

    public StringInStmt(final StringValue expr, final boolean not, final List<StringValue> valList) {
        super(not);
        this.expr = expr;
        this.valList = valList;
    }

    protected StringValue getExpr() {
        return expr;
    }

    private List<StringValue> getValList() {
        return valList;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        for (final StringValue val : this.getValList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new StringLiteral(this.getExpr().getValue(context));
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
    public boolean isContant() {
        return this.getExpr().isContant() && this.listIsConstant();
    }

    private boolean optimizeList(final EvalContext context) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue num : this.getValList()) {
            if (num.optimizeForConstants(context)) {
                newvalList.add(new StringLiteral(num.getValue(context)));
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

        final String attribVal = this.getExpr().getValue(context);
        for (final StringValue obj : this.getValList()) {
            final String val = obj.getValue(context);
            if (attribVal.equals(val))
                return true;
        }

        return false;
    }

    private boolean listIsConstant() {

        for (final StringValue val : this.getValList()) {
            if (!val.isContant())
                return false;
        }
        return true;
    }

}