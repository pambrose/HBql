package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericBetweenStmt extends GenericNotValue {

    protected GenericBetweenStmt(final boolean not, final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(not, Arrays.asList(arg0, arg1, arg2));
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        HUtil.validateParentClass(this,
                                  clazz,
                                  this.getArg(0).validateTypes(this, false),
                                  this.getArg(1).validateTypes(this, false),
                                  this.getArg(2).validateTypes(this, false));
        return BooleanValue.class;
    }

    @Override
    public String asString() {
        return this.getArg(0).asString() + notAsString() + " BETWEEN "
               + this.getArg(1).asString() + " AND " + this.getArg(2).asString();
    }
}
