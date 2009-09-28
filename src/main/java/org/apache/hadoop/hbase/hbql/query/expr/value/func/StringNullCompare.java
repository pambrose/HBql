package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class StringNullCompare extends GenericNullCompare {

    public StringNullCompare(final boolean not, final GenericValue arg0) {
        super(TypeSignature.Type.STRINGNULL.getTypeSignature(), not, arg0);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        final String val = (String)this.getArg(0).getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }

}