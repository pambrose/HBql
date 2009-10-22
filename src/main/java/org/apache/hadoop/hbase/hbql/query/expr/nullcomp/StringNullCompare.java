package org.apache.hadoop.hbase.hbql.query.expr.nullcomp;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class StringNullCompare extends GenericNullCompare {

    public StringNullCompare(final boolean not, final GenericValue arg0) {
        super(Type.STRINGNULL, not, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        final String val = (String)this.getArg(0).getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }
}