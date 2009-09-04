package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.IntegerLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class IntegerCompare extends CompareExpr implements PredicateExpr {

    private IntegerValue expr1 = null, expr2 = null;

    public IntegerCompare(final IntegerValue expr1, final OP op, final IntegerValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    private IntegerValue getExpr1() {
        return expr1;
    }

    private IntegerValue getExpr2() {
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
            this.expr1 = new IntegerLiteral(this.getExpr1().getValue(context));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(context))
            this.expr2 = new IntegerLiteral(this.getExpr2().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final EvalContext context) throws HPersistException {

        final int val1 = this.getExpr1().getValue(context).intValue();
        final int val2 = this.getExpr2().getValue(context).intValue();

        switch (this.getOp()) {
            case EQ:
                return val1 == val2;
            case GT:
                return val1 > val2;
            case GTEQ:
                return val1 >= val2;
            case LT:
                return val1 < val2;
            case LTEQ:
                return val1 <= val2;
            case NOTEQ:
                return val1 != val2;
        }

        throw new HPersistException("Error in NumberCompare.evaluate()");
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

}