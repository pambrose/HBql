package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.IntegerValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntegerLiteral extends GenericLiteral implements IntegerValue {

    private final Integer value;

    public IntegerLiteral(final Integer value) {
        this.value = value;
    }

    @Override
    public Integer getValue(final Object object) {
        return this.value;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return IntegerValue.class;
    }

    @Override
    public String asString() {
        return "" + this.value;
    }
}