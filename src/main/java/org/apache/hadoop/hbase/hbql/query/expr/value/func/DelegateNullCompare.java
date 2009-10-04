package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DelegateNullCompare extends GenericNullCompare {

    private GenericNullCompare typedExpr = null;

    public DelegateNullCompare(final boolean not, final GenericValue expr) {
        super(null, not, expr);
    }

    private GenericNullCompare getTypedExpr() {
        return typedExpr;
    }

    private void setTypedExpr(final GenericNullCompare typedExpr) {
        this.typedExpr = typedExpr;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        this.validateParentClass(StringValue.class, this.getArg(0).validateTypes(this, false));

        this.setTypedExpr(new StringNullCompare(this.isNot(), this.getArg(0)));

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return this.getTypedExpr().getValue(object);
    }
}