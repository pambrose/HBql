package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ShortValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ShortLiteral extends GenericLiteral implements ShortValue {

    private final Short value;

    public ShortLiteral(final Short value) {
        this.value = value;
    }

    @Override
    public Short getValue(final Object object) {
        return this.value;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return ShortValue.class;
    }

    @Override
    public String asString() {
        return "" + this.value;
    }
}