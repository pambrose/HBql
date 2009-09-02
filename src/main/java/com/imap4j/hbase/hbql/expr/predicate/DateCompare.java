package com.imap4j.hbase.hbql.expr.predicate;

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
 * Time: 10:30:32 PM
 */
public class DateCompare extends CompareExpr implements PredicateExpr {

    private DateValue expr1 = null, expr2 = null;

    public DateCompare(final DateValue expr1, final OP op, final DateValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    private DateValue getExpr1() {
        return expr1;
    }

    private DateValue getExpr2() {
        return expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(context))
            this.expr1 = new DateLiteral(this.getExpr1().getValue(context));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(context))
            this.expr2 = new DateLiteral(this.getExpr2().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {

        final Date val1 = this.getExpr1().getValue(context);
        final Date val2 = this.getExpr2().getValue(context);

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
        throw new HPersistException("Error in DateCompare.evaluate()");
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

}