package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringBetweenStmt extends GenericBetweenStmt {

    public StringBetweenStmt(final GenericValue arg0,
                             final boolean not,
                             final GenericValue arg1,
                             final GenericValue arg2) {
        super(new TypeSignature(BooleanValue.class, StringValue.class, StringValue.class, StringValue.class),
              not,
              arg0,
              arg1,
              arg2);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final String strval = (String)this.getArg(0).getValue(object);
        final boolean retval = strval.compareTo((String)this.getArg(1).getValue(object)) >= 0
                               && strval.compareTo((String)this.getArg(2).getValue(object)) <= 0;

        return (this.isNot()) ? !retval : retval;
    }
}