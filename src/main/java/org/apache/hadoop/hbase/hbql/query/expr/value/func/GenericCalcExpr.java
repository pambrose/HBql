package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
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

    protected Class<? extends ValueExpr> validateType(final Class<? extends ValueExpr> clazz, final String caller) throws HBqlException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (HUtil.isParentClass(clazz, type1, type2))
            throw new HBqlException("Invalid types " + type1.getName() + " " + type2.getName() + " in " + caller);

        return clazz;
    }
}
