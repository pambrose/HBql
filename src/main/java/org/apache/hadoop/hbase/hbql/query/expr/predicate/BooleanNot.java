package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

public class BooleanNot extends GenericExpr implements BooleanValue {

    private final boolean not;

    public BooleanNot(final boolean not, final BooleanValue arg0) {
        super(null, arg0);
        this.not = not;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        this.validateParentClass(BooleanValue.class, this.getArg(0).validateTypes(this, false));
        return BooleanValue.class;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        if (!this.isAConstant())
            return this;
        else
            try {
                return new BooleanLiteral(this.getValue(null));
            }
            catch (ResultMissingColumnException e) {
                throw new InternalErrorException();
            }
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        final boolean retval = (Boolean)this.getArg(0).getValue(object);
        return (this.not) ? !retval : retval;
    }

    public String asString() {
        return (this.not ? "NOT " : "") + this.getArg(0).asString();
    }
}
