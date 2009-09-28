package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateTernary extends GenericTernary implements DateValue {

    public DateTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(TypeSignature.Type.DATETERNARY.getTypeSignature(), arg0, arg1, arg2);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {
        return (Long)super.getValue(object);
    }
}