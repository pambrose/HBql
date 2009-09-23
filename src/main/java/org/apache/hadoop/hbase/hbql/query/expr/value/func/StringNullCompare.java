package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class StringNullCompare extends GenericNullCompare<StringValue> {

    public StringNullCompare(final boolean not, final StringValue expr) {
        super(not, expr);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.setExpr((StringValue)this.getExpr().getOptimizedValue());
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        final String val = this.getExpr().getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }

}