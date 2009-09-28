package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class BooleanNot extends GenericExpr implements BooleanValue {

    private final boolean not;

    public BooleanNot(final boolean not, final BooleanValue arg0) {
        super(Arrays.asList((GenericValue)arg0));
        this.not = not;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        HUtil.validateParentClass(this, BooleanValue.class, this.getArg(0).validateTypes(this, false));
        return BooleanValue.class;
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        final boolean retval = (Boolean)this.getArg(0).getValue(object);
        return (this.not) ? !retval : retval;
    }

    @Override
    public String asString() {
        return (this.not ? "NOT " : "") + this.getArg(0).asString();
    }

}
