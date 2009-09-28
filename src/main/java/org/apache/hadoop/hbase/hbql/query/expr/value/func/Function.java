package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.IntegerLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public class Function implements GenericValue {

    public static enum Type {
        // Return Strings
        TRIM(StringValue.class, (List)Arrays.asList(StringValue.class)),
        LOWER(StringValue.class, (List)Arrays.asList(StringValue.class)),
        UPPER(StringValue.class, (List)Arrays.asList(StringValue.class)),
        CONCAT(StringValue.class, (List)Arrays.asList(StringValue.class, StringValue.class)),
        REPLACE(StringValue.class, (List)Arrays.asList(StringValue.class, StringValue.class, StringValue.class)),
        SUBSTRING(StringValue.class, (List)Arrays.asList(StringValue.class, NumberValue.class, NumberValue.class)),

        // Return Numbers
        LENGTH(NumberValue.class, (List)Arrays.asList(StringValue.class)),
        INDEXOF(NumberValue.class, (List)Arrays.asList(StringValue.class, StringValue.class));

        private final Class<? extends GenericValue> returnType;
        private final List<Class> typeSig;

        Type(final Class<? extends GenericValue> returnType, final List<Class> typeSig) {
            this.returnType = returnType;
            this.typeSig = typeSig;
        }

        public Class<? extends GenericValue> getReturnType() {
            return returnType;
        }

        private List<Class> getTypeSig() {
            return typeSig;
        }

        public String asString() {
            return this.name();
        }

        private void validateArgs(final GenericValue parentExpr, final GenericValue[] valueExprs) throws TypeException {

            int i = 0;

            if (valueExprs.length != this.getTypeSig().size())
                throw new TypeException("Incorrect number of arguments in function " + this.name()
                                        + " in " + parentExpr.asString());

            for (final Class clazz : this.getTypeSig()) {

                final Class type = valueExprs[i].validateTypes(parentExpr, false);

                if (!HUtil.isParentClass(clazz, type))
                    throw new TypeException("Invalid type " + type.getSimpleName() + " for arg " + i + " in function "
                                            + this.name() + " in " + parentExpr.asString() + ".  Expecting type "
                                            + clazz.getSimpleName());
                i++;
            }
        }
    }

    private final Type type;
    private final GenericValue[] genericValues;

    public Function(final Type type, final GenericValue... genericValues) {
        this.type = type;
        this.genericValues = genericValues;
    }

    private GenericValue[] getGenericValues() {
        return this.genericValues;
    }

    private GenericValue getValueExpr(final int i) {
        return this.getGenericValues()[i];
    }

    private Type getFunctionType() {
        return this.type;
    }

    @Override
    public void setContext(final ExprTree context) {
        for (final GenericValue val : this.getGenericValues())
            val.setContext(context);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        this.getFunctionType().validateArgs(this, this.getGenericValues());
        return this.getFunctionType().getReturnType();
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        // First optimize all the args
        for (int i = 0; i < this.getGenericValues().length; i++) {
            this.getGenericValues()[i] = this.getValueExpr(i).getOptimizedValue();
        }

        switch (this.getFunctionType()) {

            case TRIM:
            case LOWER:
            case UPPER:
            case CONCAT:
            case REPLACE:
            case SUBSTRING: {
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case LENGTH:
            case INDEXOF: {
                return this.isAConstant() ? new IntegerLiteral((Integer)this.getValue(null)) : this;
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    private void checkForNull(final String... vals) throws HBqlException {
        for (final Object val : vals)
            if (val == null)
                throw new HBqlException("Null string for value in " + this.asString());
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {

        switch (this.getFunctionType()) {

            // Returns a string
            case TRIM: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                this.checkForNull(val);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                this.checkForNull(val);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                this.checkForNull(val);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.getValueExpr(0).getValue(object);
                final String v2 = (String)this.getValueExpr(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1 + v2;
            }

            case REPLACE: {
                final String v1 = (String)this.getValueExpr(0).getValue(object);
                final String v2 = (String)this.getValueExpr(1).getValue(object);
                final String v3 = (String)this.getValueExpr(2).getValue(object);
                this.checkForNull(v1, v2, v3);
                return v1.replace(v2, v3);
            }

            case SUBSTRING: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                final int begin = ((Number)this.getValueExpr(1).getValue(object)).intValue();
                final int end = ((Number)this.getValueExpr(2).getValue(object)).intValue();
                this.checkForNull(val);
                return val.substring(begin, end);
            }

            case LENGTH: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                this.checkForNull(val);
                return val.length();
            }

            case INDEXOF: {
                final String v1 = (String)this.getValueExpr(0).getValue(object);
                final String v2 = (String)this.getValueExpr(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1.indexOf(v2);
            }
            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    @Override
    public boolean isAConstant() throws HBqlException {
        for (final GenericValue val : this.getGenericValues())
            if (!val.isAConstant())
                return false;
        return true;
    }

    @Override
    public String asString() {

        final StringBuilder sbuf = new StringBuilder(this.getFunctionType().name() + "(");

        boolean first = true;
        for (final GenericValue val : this.getGenericValues()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(val.asString());
            first = false;
        }

        sbuf.append(")");

        return sbuf.toString();
    }
}