package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public abstract class GenericCalcExpr extends GenericTwoExprExpr implements ValueExpr {

    private final Operator op;

    public GenericCalcExpr(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected Operator getOp() {
        return op;
    }

    protected Class<? extends ValueExpr> validateType(final Class<? extends ValueExpr> clazz) throws TypeException {
        HUtil.validateParentClass(this, clazz, this.getExpr1().validateTypes(this));
        if (this.getExpr2() != null)
            HUtil.validateParentClass(this, clazz, this.getExpr2().validateTypes(this));

        return clazz;
    }

    @Override
    public String asString() {
        if (this.getOp() == Operator.NEGATIVE)
            return "-" + this.getExpr1().asString();
        else if (this.getExpr2() == null)
            return this.getExpr1().asString();
        else
            return this.getExpr1().asString() + " " + this.getOp() + " " + this.getExpr2().asString();
    }
}
