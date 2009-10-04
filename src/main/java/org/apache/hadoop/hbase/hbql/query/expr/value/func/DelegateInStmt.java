package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DelegateInStmt extends GenericInStmt {

    private GenericInStmt typedExpr = null;

    public DelegateInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    private GenericInStmt getTypedExpr() {
        return typedExpr;
    }

    private void setTypedExpr(final GenericInStmt typedExpr) {
        this.typedExpr = typedExpr;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type = this.getArg(0).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type))
            this.setTypedExpr(new StringInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else if (HUtil.isParentClass(NumberValue.class, type))
            this.setTypedExpr(new NumberInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else if (HUtil.isParentClass(DateValue.class, type))
            this.setTypedExpr(new DateInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else
            this.throwInvalidTypeException(type);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    @Override
    protected boolean evaluateList(final Object object) throws HBqlException {
        throw new HBqlException("Internal error");
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return this.getTypedExpr().getValue(object);
    }

}