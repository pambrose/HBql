package org.apache.hadoop.hbase.hbql.stmt.expr.compare;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.stmt.expr.GenericExpression;
import org.apache.hadoop.hbase.hbql.stmt.expr.Operator;
import org.apache.hadoop.hbase.hbql.stmt.expr.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public abstract class GenericCompare extends GenericExpression implements BooleanValue {

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
        try {
            this.validateParentClass(clazz,
                                     this.getArg(0).validateTypes(this, false),
                                     this.getArg(1).validateTypes(this, false));
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }

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