package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateLiteral extends GenericExpr implements DateValue {

    public DateLiteral(final GenericValue arg0, final GenericValue arg1) {
        super(null, arg0, arg1);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        this.validateParentClass(StringValue.class,
                                 this.getArg(0).validateTypes(this, false),
                                 this.getArg(1).validateTypes(this, false));
        return DateValue.class;
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return this.isAConstant() ? new DateConstant(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final String datestr = (String)this.getArg(0).getValue(object);
        final String pattern = (String)this.getArg(1).getValue(object);
        final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

        try {
            return formatter.parse(datestr).getTime();
        }
        catch (ParseException e) {
            throw new HBqlException(e.getMessage());
        }
    }

    @Override
    public String asString() {
        return "DATE" + super.asString();
    }
}