package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.stmt.Operator;

public abstract class GenericCompare extends GenericExpr implements BooleanValue {

    private final Operator operator;

    protected GenericCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(null, arg0, arg1);
        this.operator = operator;
    }

    protected Operator getOperator() {
        return this.operator;
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

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        this.validateParentClass(clazz,
                                 this.getArg(0).validateTypes(this, false),
                                 this.getArg(1).validateTypes(this, false));

        return BooleanValue.class;
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder();
        sbuf.append(this.getArg(0).asString());
        sbuf.append(" " + this.getOperator() + " ");
        sbuf.append(this.getArg(1).asString());
        return sbuf.toString();
    }
}