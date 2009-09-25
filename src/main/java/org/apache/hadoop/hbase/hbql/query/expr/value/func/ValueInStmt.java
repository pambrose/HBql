package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueInStmt extends GenericInStmt {

    private GenericInStmt typedExpr = null;

    public ValueInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> valList) {
        super(not, expr, valList);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HBqlException {

        final Class<? extends ValueExpr> type = this.getExpr().validateType();
        final Class<? extends ValueExpr> clazz;

        if (HUtil.isParentClass(StringValue.class, type)) {
            clazz = StringValue.class;
            this.typedExpr = new StringInStmt(this.getExpr(), this.isNot(), this.getValueExprList());
        }
        else if (HUtil.isParentClass(NumberValue.class, type)) {
            clazz = NumberValue.class;
            this.typedExpr = new NumberInStmt(this.getExpr(), this.isNot(), this.getValueExprList());
        }
        else if (HUtil.isParentClass(DateValue.class, type)) {
            clazz = DateValue.class;
            this.typedExpr = new DateInStmt(this.getExpr(), this.isNot(), this.getValueExprList());
        }
        else
            throw new HBqlException("Invalid type " + type.getName() + " in GenericInStmt");

        // First make sure all the types are matched
        for (final ValueExpr val : this.getValueExprList()) {
            final Class<? extends ValueExpr> valtype = val.validateType();

            if (!HUtil.isParentClass(clazz, valtype))
                throw new HBqlException("Invalid type " + type.getName() + " in GenericInStmt");
        }

        return BooleanValue.class;
    }

    @Override
    protected boolean evaluateList(final Object object) throws HBqlException {
        // Not used
        return false;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }

}