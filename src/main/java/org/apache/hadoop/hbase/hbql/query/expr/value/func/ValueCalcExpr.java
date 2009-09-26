package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueCalcExpr extends GenericCalcExpr {

    private GenericCalcExpr typedExpr = null;

    public ValueCalcExpr(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateTypes(this, false);
        final Class<? extends ValueExpr> type2 = (this.getExpr2() != null)
                                                 ? this.getExpr2().validateTypes(this, false) : null;

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            typedExpr = new StringCalcExpr(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            typedExpr = new NumberCalcExpr(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            typedExpr = new DateCalcExpr(this.getExpr1(), this.getOp(), this.getExpr2());
        else
            HUtil.throwInvalidTypeException(this, type1, type2);

        this.typedExpr.validateTypes(parentExpr, false);

        return type1;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }
}