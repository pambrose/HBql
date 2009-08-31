package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanExpr implements PredicateExpr {

    public enum OP {
        AND,
        OR
    }

    private PredicateExpr expr1 = null, expr2 = null;
    private final BooleanExpr.OP op;

    public BooleanExpr(final PredicateExpr expr1, final BooleanExpr.OP op, final PredicateExpr expr2) {
        this.expr1 = expr1;
        this.op = op;
        this.expr2 = expr2;
    }

    private PredicateExpr getExpr1() {
        return expr1;
    }

    private PredicateExpr getExpr2() {
        return expr2;
    }

    private OP getOp() {
        return op;
    }

    @Override
    public List<String> getAttribNames() {

        final List<String> retval = this.getExpr1().getAttribNames();
        retval.addAll(this.getExpr2().getAttribNames());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(context))
            this.expr1 = new BooleanLiteral(this.getExpr1().evaluate(context));
        else
            retval = false;

        if (this.getExpr2() != null) {
            if (this.getExpr2().optimizeForConstants(context))
                this.expr2 = new BooleanLiteral(this.getExpr2().evaluate(context));
            else
                retval = false;
        }

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {

        final boolean expr1val = this.getExpr1().evaluate(context);

        if (this.getExpr2() == null)
            return expr1val;

        switch (this.getOp()) {
            case OR:
                return expr1val || this.getExpr2().evaluate(context);
            case AND:
                return expr1val && this.getExpr2().evaluate(context);

            default:
                throw new HPersistException("Error in BooleanExpr.evaluate()");

        }
    }
}
