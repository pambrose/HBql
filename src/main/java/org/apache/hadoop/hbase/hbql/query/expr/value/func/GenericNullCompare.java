package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public abstract class GenericNullCompare extends GenericNotValue {

    private GenericValue expr = null;

    protected GenericNullCompare(final boolean not, final GenericValue expr) {
        super(not);
        this.expr = expr;
    }

    protected GenericValue getExpr() {
        return this.expr;
    }

    protected void setExpr(final GenericValue expr) {
        this.expr = expr;
    }

    @Override
    public boolean isAConstant() throws HBqlException {
        return this.getExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        HUtil.validateParentClass(this, clazz, this.getExpr().validateTypes(this, false));
        return BooleanValue.class;
    }

    @Override
    public String asString() {
        return this.getExpr().asString() + " IS" + notAsString() + " NULL";
    }

}