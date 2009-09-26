package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.IntegerLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringNullLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NamedParameter implements ValueExpr {

    private ExprTree context = null;
    private final String paramName;

    private ValueExpr typedExpr = null;
    private List<ValueExpr> typedExprList = null;

    public NamedParameter(final String paramName) {
        this.paramName = paramName;
    }

    public String getParamName() {
        return this.paramName;
    }

    private boolean isScalar() {
        return this.getTypedExpr() != null;
    }

    private ValueExpr getTypedExpr() {
        return this.typedExpr;
    }

    private List<ValueExpr> getTypedExprList() {
        return this.typedExprList;
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr) throws TypeException {

        if (this.getTypedExpr() == null && this.getTypedExprList() == null)
            throw new TypeException("Parameter " + this.getParamName() + " not assigned a value");

        if (this.isScalar())
            return this.getTypedExpr().getClass();

        // if it is a list, then ensure that all the types in list are consistent
        if (this.getTypedExprList().size() == 0)
            throw new TypeException("Parameter " + this.getParamName() + " not assigned a list with any values");

        // Look at the type of the first item and then make sure the rest match that one
        final ValueExpr firstval = this.getTypedExprList().get(0);
        final Class<? extends ValueExpr> clazzToMatch = HUtil.getValueDescType(firstval);

        for (final ValueExpr val : this.getTypedExprList()) {

            final Class<? extends ValueExpr> clazz = HUtil.getValueDescType(val);

            if (clazz == null)
                throw new TypeException("Parameter " + this.getParamName()
                                        + " assigned a collection value with invalid type "
                                        + firstval.getClass().getSimpleName());

            if (!clazz.equals(clazzToMatch))
                throw new TypeException("Parameter " + this.getParamName()
                                        + " assigned a collection value with type "
                                        + firstval.getClass().getSimpleName()
                                        + " which is inconsistent with the type of the first element");
        }

        return clazzToMatch;
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        if (this.isScalar())
            return this.getTypedExpr().getValue(object);
        else
            return this.getTypedExprList();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.context = context;
        this.context.addNamedParameter(this);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        return this;
    }

    @Override
    public boolean isAConstant() {
        return false;
    }

    public void setParameter(final Object val) throws HBqlException {

        if (val != null && HUtil.isParentClass(Collection.class, val.getClass())) {
            this.typedExprList = Lists.newArrayList();
            for (final Object elem : (Collection)val)
                this.typedExprList.add(this.getValueExpr(elem));
        }
        else {
            this.typedExpr = this.getValueExpr(val);
        }
    }

    private ValueExpr getValueExpr(final Object val) throws TypeException {

        if (val == null)
            return new StringNullLiteral();

        if (val instanceof Boolean)
            return new BooleanLiteral((Boolean)val);

        if (val instanceof String)
            return new StringLiteral((String)val);

        if (val instanceof Integer)
            return new IntegerLiteral((Integer)val);

        if (val instanceof Date)
            return new DateLiteral((Date)val);

        throw new TypeException("Parameter " + this.getParamName()
                                + " assigned an unsupported type " + val.getClass().getSimpleName());
    }


    @Override
    public String asString() {
        return this.getParamName();
    }
}