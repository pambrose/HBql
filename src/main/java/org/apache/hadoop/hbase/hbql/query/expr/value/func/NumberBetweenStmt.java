package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberBetweenStmt extends GenericBetweenStmt {

    public NumberBetweenStmt(final GenericValue arg0,
                             final boolean not,
                             final GenericValue arg1,
                             final GenericValue arg2) {
        super(TypeSignature.Type.NUMBERBETWEEN.getTypeSignature(), not, arg0, arg1, arg2);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final long numval = ((Number)this.getArg(0).getValue(object)).longValue();
        final boolean retval = numval >= ((Number)this.getArg(1).getValue(object)).longValue()
                               && numval <= ((Number)this.getArg(2).getValue(object)).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}