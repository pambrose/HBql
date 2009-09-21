package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringLiteral extends GenericLiteral implements StringValue {

    private final String value;

    public StringLiteral(final String value) {
        this.value = value;
    }

    @Override
    public String getValue(final Object object) {
        return this.value;
    }
}