package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

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

    private List<DateValue> getValueList() {
        return valList;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        for (final DateValue val : this.getValueList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new DateLiteral(this.getExpr().getValue(object));
        else
            retval = false;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {
        final boolean retval = this.evaluateList(object);
        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.listIsConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr().setSchema(schema);
        for (final DateValue value : this.getValueList())
            value.setSchema(schema);
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<DateValue> newvalList = Lists.newArrayList();

        for (final DateValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new DateLiteral(val.getValue(object)));
            }
            else {
                newvalList.add(val);
                retval = false;
            }
        }

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);

        return retval;
    }

    private boolean evaluateList(final Object object) throws HPersistException {

        final long attribVal = this.getExpr().getValue(object);
        for (final DateValue obj : this.getValueList()) {
            final long val = obj.getValue(object);
            if (attribVal == val)
                return true;
        }

        return false;
    }

    private boolean listIsConstant() {

        for (final DateValue val : this.getValueList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }

}