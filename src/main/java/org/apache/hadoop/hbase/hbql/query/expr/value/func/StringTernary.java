package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class StringTernary extends GenericTernary {

    public StringTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(arg0, arg1, arg2);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(StringValue.class);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        this.setArg(0, this.getArg(0).getOptimizedValue());
        this.setArg(1, this.getArg(1).getOptimizedValue());
        this.setArg(2, this.getArg(2).getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public String getValue(final Object object) throws HBqlException {
        return (String)super.getValue(object);
    }
}