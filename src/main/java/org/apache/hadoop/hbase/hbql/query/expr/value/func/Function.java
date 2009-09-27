package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
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
public class Function implements ValueExpr {

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

        private final Class<? extends ValueExpr> returnType;
        private final List<Class> typeSig;

        Type(final Class<? extends ValueExpr> returnType, final List<Class> typeSig) {
            this.returnType = returnType;
            this.typeSig = typeSig;
        }

        public Class<? extends ValueExpr> getReturnType() {
            return returnType;
        }

        private List<Class> getTypeSig() {
            return typeSig;
        }

        public String asString() {
            return this.name();
        }

        public void validateArgs(final ValueExpr parentExpr, final ValueExpr[] valueExprs) throws TypeException {

            int i = 0;

            if (valueExprs.length != this.getTypeSig().size())
                throw new TypeException("Incorrect number of arguments in function " + this.name());

            for (final Class clazz : this.getTypeSig()) {

                final Class type = valueExprs[i].validateTypes(parentExpr, false);

                if (!HUtil.isParentClass(clazz, type))
                    throw new TypeException("Invalid type " + type.getSimpleName() + " for arg " + i + " in function "
                                            + this.name() + ".  Expecting type " + clazz.getSimpleName());
                i++;
            }
        }
    }

    private final Type type;
    private final ValueExpr[] valueExprs;

    public Function(final Type type, final ValueExpr... valueExprs) {
        this.type = type;
        this.valueExprs = valueExprs;
    }

    private ValueExpr[] getValueExprs() {
        return valueExprs;
    }

    private ValueExpr getValueExpr(final int i) {
        return this.getValueExprs()[i];
    }

    private Type getFunctionType() {
        return this.type;
    }

    @Override
    public void setContext(final ExprTree context) {
        for (final ValueExpr valExpr : this.getValueExprs())
            valExpr.setContext(context);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        switch (this.getFunctionType()) {
            case TRIM:
            case LOWER:
            case UPPER:
            case CONCAT:
            case REPLACE:
            case SUBSTRING:
            case LENGTH:
            case INDEXOF:
                this.getFunctionType().validateArgs(this, this.getValueExprs());
                return this.getFunctionType().getReturnType();
            default:
                throw new TypeException("Invalid function " + this.getFunctionType() + " in " + this.asString());
        }
    }

    private void getOptimizedValue(final int i) throws HBqlException {
        this.getValueExprs()[i] = this.getValueExpr(i).getOptimizedValue();
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {

        switch (this.getFunctionType()) {

            // Returns a string
            case TRIM: {
                this.getOptimizedValue(0);
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case LOWER: {
                this.getOptimizedValue(0);
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case UPPER: {
                this.getOptimizedValue(0);
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case CONCAT: {
                this.getOptimizedValue(0);
                this.getOptimizedValue(1);
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case REPLACE: {
                this.getOptimizedValue(0);
                this.getOptimizedValue(1);
                this.getOptimizedValue(2);
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case SUBSTRING: {
                this.getOptimizedValue(0);
                this.getOptimizedValue(1);
                this.getOptimizedValue(2);
                return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;
            }

            case LENGTH: {
                this.getOptimizedValue(0);
                return this.isAConstant() ? new IntegerLiteral((Integer)this.getValue(null)) : this;
            }

            case INDEXOF: {
                this.getOptimizedValue(0);
                this.getOptimizedValue(1);
                return this.isAConstant() ? new IntegerLiteral((Integer)this.getValue(null)) : this;
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {

        switch (this.getFunctionType()) {

            // Returns a string
            case TRIM: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.getValueExpr(0).getValue(object);
                final String v2 = (String)this.getValueExpr(1).getValue(object);
                return v1 + v2;
            }

            case REPLACE: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                final String v2 = (String)this.getValueExpr(1).getValue(object);
                final String v3 = (String)this.getValueExpr(2).getValue(object);
                return val.replace(v2, v3);
            }

            case SUBSTRING: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                final int begin = ((Number)this.getValueExpr(1).getValue(object)).intValue();
                final int end = ((Number)this.getValueExpr(2).getValue(object)).intValue();
                return val.substring(begin, end);
            }

            case LENGTH: {
                final String val = (String)this.getValueExpr(0).getValue(object);
                if (val == null)
                    return 0;
                else
                    return val.length();
            }

            case INDEXOF: {
                final String val1 = (String)this.getValueExpr(0).getValue(object);
                final String val2 = (String)this.getValueExpr(1).getValue(object);
                if (val1 == null || val2 == null)
                    return -1;
                else
                    return val1.indexOf(val2);
            }
            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    @Override
    public boolean isAConstant() throws HBqlException {

        switch (this.getFunctionType()) {

            case TRIM:
                return this.getValueExpr(0).isAConstant();

            case LOWER:
                return this.getValueExpr(0).isAConstant();

            case UPPER:
                return this.getValueExpr(0).isAConstant();

            case CONCAT:
                return this.getValueExpr(0).isAConstant()
                       && this.getValueExpr(1).isAConstant();

            case REPLACE:
                return this.getValueExpr(0).isAConstant()
                       && this.getValueExpr(1).isAConstant()
                       && this.getValueExpr(2).isAConstant();

            case SUBSTRING:
                return this.getValueExpr(0).isAConstant()
                       && this.getValueExpr(1).isAConstant()
                       && this.getValueExpr(2).isAConstant();

            case LENGTH:
                return this.getValueExpr(0).isAConstant();

            case INDEXOF:
                return this.getValueExpr(0).isAConstant()
                       && this.getValueExpr(1).isAConstant();

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }

    }

    @Override
    public String asString() {

        final StringBuilder sbuf = new StringBuilder(this.getFunctionType().name() + "(");

        switch (this.getFunctionType()) {

            case TRIM:
                sbuf.append(this.getValueExpr(0).asString());
                break;

            case LOWER:
                sbuf.append(this.getValueExpr(0).asString());
                break;

            case UPPER:
                sbuf.append(this.getValueExpr(0).asString());
                break;

            case CONCAT:
                sbuf.append(this.getValueExpr(0).asString());
                sbuf.append(", ");
                sbuf.append(this.getValueExpr(1).asString());
                break;

            case REPLACE:
                sbuf.append(this.getValueExpr(0).asString());
                sbuf.append(", ");
                sbuf.append(this.getValueExpr(1).asString());
                sbuf.append(", ");
                sbuf.append(this.getValueExpr(2).asString());
                break;

            case SUBSTRING:
                sbuf.append(this.getValueExpr(0).asString());
                sbuf.append(", ");
                sbuf.append(this.getValueExpr(1).asString());
                sbuf.append(", ");
                sbuf.append(this.getValueExpr(2).asString());
                break;

            case LENGTH:
                sbuf.append(this.getValueExpr(0).asString());
                break;

            case INDEXOF:
                sbuf.append(this.getValueExpr(0).asString());
                sbuf.append(", ");
                sbuf.append(this.getValueExpr(1).asString());
                break;
            default:
                sbuf.append("Unknown Function: " + this.getFunctionType());
        }

        sbuf.append(")");

        return sbuf.toString();
    }
}