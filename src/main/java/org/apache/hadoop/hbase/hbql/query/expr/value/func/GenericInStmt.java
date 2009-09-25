package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericInStmt extends GenericNotValue {

    private ValueExpr expr = null;
    private final List<ValueExpr> valueList;

    protected GenericInStmt(final boolean not, final ValueExpr expr, final List<ValueExpr> valueList) {
        super(not);
        this.expr = expr;
        this.valueList = valueList;
    }

    protected ValueExpr getExpr() {
        return expr;
    }

    protected void setExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    protected List<ValueExpr> getValueList() {
        return valueList;
    }

    protected abstract boolean evaluateList(final Object object) throws HBqlException;

    private void optimizeList() throws HBqlException {

        final List<ValueExpr> newvalList = Lists.newArrayList();

        for (final ValueExpr val : this.getValueList())
            newvalList.add(val.getOptimizedValue());

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        this.setExpr(this.getExpr().getOptimizedValue());
        this.optimizeList();
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        final boolean retval = this.evaluateList(object);
        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.listIsConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
        for (final ValueExpr valueExpr : this.getValueList())
            valueExpr.setContext(context);
    }

    private boolean listIsConstant() {
        for (final ValueExpr val : this.getValueList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HBqlException {

        final Class<? extends ValueExpr> type = this.getExpr().validateType();
        final Class<? extends ValueExpr> clazz;

        if (HUtil.isParentClass(StringValue.class, type))
            clazz = StringValue.class;
        else if (HUtil.isParentClass(NumberValue.class, type))
            clazz = NumberValue.class;
        else if (HUtil.isParentClass(DateValue.class, type))
            clazz = DateValue.class;
        else
            throw new HBqlException("Invalid type " + type.getName() + " in GenericInStmt");

        // First make sure all the types are matched
        for (final ValueExpr val : this.getValueList()) {
            final Class<? extends ValueExpr> valtype = val.validateType();

            if (!HUtil.isParentClass(clazz, valtype))
                throw new HBqlException("Invalid type " + type.getName() + " in GenericInStmt");
        }

        return BooleanValue.class;
    }

}