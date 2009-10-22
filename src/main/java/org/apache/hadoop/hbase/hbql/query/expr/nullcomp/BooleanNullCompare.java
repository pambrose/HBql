package org.apache.hadoop.hbase.hbql.query.expr.nullcomp;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class BooleanNullCompare extends GenericNullCompare {

    public BooleanNullCompare(final boolean not, final GenericValue arg0) {
        super(GenericExpr.Type.BOOLEANNULL, not, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        final Boolean val = (Boolean)this.getArg(0).getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }
}