package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringBetweenStmt extends GenericBetweenStmt {

    public StringBetweenStmt(final GenericValue expr, final boolean not, final GenericValue lower, final GenericValue upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(StringValue.class);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final String strval = (String)this.getArg(0).getValue(object);
        final boolean retval = strval.compareTo((String)this.getArg(1).getValue(object)) >= 0
                               && strval.compareTo((String)this.getArg(2).getValue(object)) <= 0;

        return (this.isNot()) ? !retval : retval;
    }
}