package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

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

    private List<StringValue> getValueList() {
        return valList;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        for (final StringValue val : this.getValueList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new StringLiteral(this.getExpr().getValue(object));
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
        for (final StringValue val : this.getValueList())
            val.setSchema(schema);
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new StringLiteral(val.getValue(object)));
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

        final String attribVal = this.getExpr().getValue(object);
        for (final StringValue obj : this.getValueList()) {
            final String val = obj.getValue(object);
            if (attribVal.equals(val))
                return true;
        }

        return false;
    }

    private boolean listIsConstant() {

        for (final StringValue val : this.getValueList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }

}