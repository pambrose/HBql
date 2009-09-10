package com.imap4j.hbase.hbql.expr.predicate;

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
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.expr1 = new DateLiteral(this.getExpr1().getValue(object));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.expr2 = new DateLiteral(this.getExpr2().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final long val1 = this.getExpr1().getValue(object);
        final long val2 = this.getExpr2().getValue(object);

        switch (this.getOp()) {
            case EQ:
                return val1 == val2;
            case NOTEQ:
                return val1 != val2;
            case GT:
                return val1 > val2;
            case GTEQ:
                return val1 >= val2;
            case LT:
                return val1 < val2;
            case LTEQ:
                return val1 <= val2;
        }
        throw new HPersistException("Error in DateCompare.evaluate()");
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