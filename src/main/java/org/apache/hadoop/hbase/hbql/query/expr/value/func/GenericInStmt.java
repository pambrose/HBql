package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
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

    private GenericValue expr = null;
    private final List<GenericValue> valueExprList;

    protected GenericInStmt(final boolean not, final GenericValue expr, final List<GenericValue> valueExprList) {
        super(not);
        this.expr = expr;
        this.valueExprList = valueExprList;
    }

    protected GenericValue getExpr() {
        return this.expr;
    }

    protected void setExpr(final GenericValue expr) {
        this.expr = expr;
    }

    protected List<GenericValue> getValueExprList() {
        return this.valueExprList;
    }

    protected abstract boolean evaluateList(final Object object) throws HBqlException;

    private void optimizeList() throws HBqlException {

        final List<GenericValue> newvalList = Lists.newArrayList();

        for (final GenericValue val : this.getValueExprList())
            newvalList.add(val.getOptimizedValue());

        // Swap new values to list
        this.getValueExprList().clear();
        this.getValueExprList().addAll(newvalList);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
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
    public boolean isAConstant() throws HBqlException {
        return this.getExpr().isAConstant() && this.listIsConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
        for (final GenericValue valueExpr : this.getValueExprList())
            valueExpr.setContext(context);
    }

    private boolean listIsConstant() {
        for (final GenericValue val : this.getValueExprList()) {
            try {
                if (!val.isAConstant())
                    return false;
            }
            catch (HBqlException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type = this.getExpr().validateTypes(this, false);
        final Class<? extends GenericValue> inClazz;

        if (HUtil.isParentClass(StringValue.class, type))
            inClazz = StringValue.class;
        else if (HUtil.isParentClass(NumberValue.class, type))
            inClazz = NumberValue.class;
        else if (HUtil.isParentClass(DateValue.class, type))
            inClazz = DateValue.class;
        else {
            inClazz = null;
            HUtil.throwInvalidTypeException(this, type);
        }

        // First make sure all the types are matched
        for (final GenericValue inVal : this.getValueExprList())
            HUtil.validateParentClass(this, inClazz, inVal.validateTypes(this, true));

        return BooleanValue.class;
    }

    @Override
    public String asString() {
        final StringBuilder sbuf = new StringBuilder(this.getExpr().asString() + notAsString() + " IN (");

        boolean first = true;
        for (final GenericValue valueExpr : this.getValueExprList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(valueExpr.asString());
            first = false;
        }
        sbuf.append(")");
        return sbuf.toString();
    }

}