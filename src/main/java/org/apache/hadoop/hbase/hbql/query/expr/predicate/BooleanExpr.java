package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.ExprSchema;

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
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.expr1 = new BooleanLiteral(this.getExpr1().evaluate(object));
        else
            retval = false;

        if (this.getExpr2() != null) {
            if (this.getExpr2().optimizeForConstants(object))
                this.expr2 = new BooleanLiteral(this.getExpr2().evaluate(object));
            else
                retval = false;
        }

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final boolean expr1val = this.getExpr1().evaluate(object);

        if (this.getExpr2() == null)
            return expr1val;

        switch (this.getOp()) {
            case OR:
                return expr1val || this.getExpr2().evaluate(object);
            case AND:
                return expr1val && this.getExpr2().evaluate(object);

            default:
                throw new HPersistException("Error in BooleanExpr.evaluate()");

        }
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
