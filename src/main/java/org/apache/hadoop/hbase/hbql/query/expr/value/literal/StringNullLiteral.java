package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringNullLiteral extends GenericLiteral<StringValue> {

    public StringNullLiteral() {
    }

    @Override
    public String getValue(final Object object) {
        return null;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {
        return StringValue.class;
    }

}