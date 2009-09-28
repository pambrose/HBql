package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.ExprArgs;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.IntegerLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

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
        TRIM(StringValue.class, StringValue.class),
        LOWER(StringValue.class, StringValue.class),
        UPPER(StringValue.class, StringValue.class),
        CONCAT(StringValue.class, StringValue.class, StringValue.class),
        REPLACE(StringValue.class, StringValue.class, StringValue.class, StringValue.class),
        SUBSTRING(StringValue.class, StringValue.class, NumberValue.class, NumberValue.class),

        // Return Numbers
        LENGTH(NumberValue.class, StringValue.class),
        INDEXOF(NumberValue.class, StringValue.class, StringValue.class);

        private final TypeSignature typeSignature;

        Type(final Class<? extends GenericValue> returnType, final Class<? extends GenericValue>... clazzes) {
            final List<Class<? extends GenericValue>> typeSig = Lists.newArrayList();
            for (final Class<? extends GenericValue> clazz : clazzes)
                typeSig.add(clazz);
            this.typeSignature = new TypeSignature(returnType, typeSig);
        }

        private TypeSignature getTypeSignature() {
            return typeSignature;
        }
    }

    private final Type functionType;
    private final ExprArgs exprArgs;

    public Function(final Type functionType, final GenericValue... genericValues) {
        this.functionType = functionType;
        this.exprArgs = new ExprArgs(Arrays.asList(genericValues));
    }

    private Type getFunctionType() {
        return this.functionType;
    }

    private ExprArgs getArgs() {
        return this.exprArgs;
    }

    private GenericValue getArg(final int i) {
        return this.getArgs().getArg(i);
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getArgs().setContext(context);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        int i = 0;
        if (this.getArgs().size() != this.getFunctionType().getTypeSignature().size())
            throw new TypeException("Incorrect number of arguments in function " + this.getFunctionType().name()
                                    + " in " + this.asString());

        for (final Class<? extends GenericValue> clazz : this.getFunctionType().getTypeSignature().getSignatureArgs()) {
            final Class<? extends GenericValue> type = this.getArg(i).validateTypes(this, false);
            if (!HUtil.isParentClass(clazz, type))
                throw new TypeException("Invalid type " + type.getSimpleName() + " for arg " + i + " in function "
                                        + this.getFunctionType().name() + " in "
                                        + this.asString() + ".  Expecting type " + clazz.getSimpleName());
            i++;
        }

        return this.getFunctionType().getTypeSignature().getSignatureReturnType();
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        // First optimize all the args
        this.getArgs().optimizeArgs();

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
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1 + v2;
            }

            case REPLACE: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                final String v3 = (String)this.getArg(2).getValue(object);
                this.checkForNull(v1, v2, v3);
                return v1.replace(v2, v3);
            }

            case SUBSTRING: {
                final String val = (String)this.getArg(0).getValue(object);
                final int begin = ((Number)this.getArg(1).getValue(object)).intValue();
                final int end = ((Number)this.getArg(2).getValue(object)).intValue();
                this.checkForNull(val);
                return val.substring(begin, end);
            }

            case LENGTH: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.length();
            }

            case INDEXOF: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1.indexOf(v2);
            }
            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    @Override
    public boolean isAConstant() throws HBqlException {
        return this.getArgs().isAConstant();
    }

    @Override
    public String asString() {
        return this.getFunctionType().name() + "(" + this.getArgs().asString() + ")";
    }
}