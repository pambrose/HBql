package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DelegateNullCompare extends GenericNullCompare {

    private GenericNullCompare typedExpr = null;

    public DelegateNullCompare(final boolean not, final GenericValue expr) {
        super(not, expr);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        HUtil.validateParentClass(this, StringValue.class, this.getArg(0).validateTypes(this, false));

        this.typedExpr = new StringNullCompare(this.isNot(), this.getArg(0));

        return this.typedExpr.validateTypes(parentExpr, false);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }
}