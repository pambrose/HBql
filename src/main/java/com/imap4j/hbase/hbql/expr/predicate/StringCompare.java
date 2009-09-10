package com.imap4j.hbase.hbql.expr.predicate;

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
 * Time: 10:30:32 PM
 */
public class StringCompare extends CompareExpr implements PredicateExpr {

    private StringValue expr1 = null, expr2 = null;

    public StringCompare(final StringValue expr1, final OP op, final StringValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    private StringValue getExpr1() {
        return this.expr1;
    }

    private StringValue getExpr2() {
        return this.expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {

        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.expr1 = new StringLiteral(this.getExpr1().getValue(object));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.expr2 = new StringLiteral(this.getExpr2().getValue(object));
        else
            retval = false;

        return retval;
    }


    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final String val1 = this.getExpr1().getValue(object);
        final String val2 = this.getExpr2().getValue(object);

        switch (this.getOp()) {
            case EQ:
                return val1.equals(val2);
            case NOTEQ:
                return !val1.equals(val2);
            case GT:
                return val1.compareTo(val2) > 0;
            case GTEQ:
                return val1.compareTo(val2) >= 0;
            case LT:
                return val1.compareTo(val2) < 0;
            case LTEQ:
                return val1.compareTo(val2) <= 0;
        }

        throw new HPersistException("Error in StringCompare.evaluate()");
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr1().setSchema(schema);
        this.getExpr2().setSchema(schema);
    }
}
