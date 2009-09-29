package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericInStmt extends GenericNotValue {

    protected GenericInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(Type.GENERICINSTMT, not, arg0, inList);
    }

    protected abstract boolean evaluateList(final Object object) throws HBqlException;

    protected List<GenericValue> getInList() {
        return this.getSubArgs(1);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        final boolean retval = this.evaluateList(object);
        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type = this.getArg(0).validateTypes(this, false);

        final Class<? extends GenericValue> inClazz;

        if (HUtil.isParentClass(StringValue.class, type))
            inClazz = StringValue.class;
        else if (HUtil.isParentClass(NumberValue.class, type))
            inClazz = NumberValue.class;
        else if (HUtil.isParentClass(DateValue.class, type))
            inClazz = DateValue.class;
        else {
            inClazz = null;
            this.throwInvalidTypeException(type);
        }

        // Make sure all the types are matched
        for (final GenericValue inVal : this.getInList())
            this.validateParentClass(inClazz, inVal.validateTypes(this, true));

        return BooleanValue.class;
    }

    @Override
    public String asString() {
        final StringBuilder sbuf = new StringBuilder(this.getArg(0).asString() + notAsString() + " IN (");

        boolean first = true;
        for (final GenericValue valueExpr : this.getInList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(valueExpr.asString());
            first = false;
        }
        sbuf.append(")");
        return sbuf.toString();
    }

}