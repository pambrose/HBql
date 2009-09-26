package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class ValueNullCompare extends GenericNullCompare {

    private GenericNullCompare typedExpr = null;

    public ValueNullCompare(final boolean not, final ValueExpr expr) {
        super(not, expr);
    }

    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr) throws TypeException {

        HUtil.validateParentClass(this, StringValue.class, this.getExpr().validateTypes(this));

        this.typedExpr = new StringNullCompare(this.isNot(), this.getExpr());

        return BooleanValue.class;
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