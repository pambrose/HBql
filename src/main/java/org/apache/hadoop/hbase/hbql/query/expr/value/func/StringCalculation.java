package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class StringCalculation extends GenericCalculation {

    public StringCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(new TypeSignature(StringValue.class, StringValue.class, StringValue.class), arg0, operator, arg1);
    }

    @Override
    public String getValue(final Object object) throws HBqlException {

        final String val1 = (String)this.getArg(0).getValue(object);
        final String val2 = (String)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case PLUS:
                return val1 + val2;
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}