package org.apache.hadoop.hbase.hbql.stmt.expr.function;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.stmt.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.stmt.expr.TypeSignature;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.IntegerValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.LongValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.ShortValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.stmt.util.HUtil;

import java.util.List;
import java.util.Random;

public abstract class Function extends GenericExpr {

    static Random randomVal = new Random();

    public static enum FunctionType {

        // Dealt with in DateFunction
        DATEINTERVAL(new TypeSignature(DateValue.class, LongValue.class)),
        DATECONSTANT(new TypeSignature(DateValue.class)),

        // Date functions
        DATE(new TypeSignature(DateValue.class, StringValue.class, StringValue.class)),
        LONGTODATE(new TypeSignature(DateValue.class, LongValue.class)),
        RANDOMDATE(new TypeSignature(DateValue.class)),

        // String functions
        TRIM(new TypeSignature(StringValue.class, StringValue.class)),
        LOWER(new TypeSignature(StringValue.class, StringValue.class)),
        UPPER(new TypeSignature(StringValue.class, StringValue.class)),
        CONCAT(new TypeSignature(StringValue.class, StringValue.class, StringValue.class)),
        REPLACE(new TypeSignature(StringValue.class, StringValue.class, StringValue.class, StringValue.class)),
        SUBSTRING(new TypeSignature(StringValue.class, StringValue.class, IntegerValue.class, IntegerValue.class)),
        ZEROPAD(new TypeSignature(StringValue.class, LongValue.class, IntegerValue.class)),
        REPEAT(new TypeSignature(StringValue.class, StringValue.class, IntegerValue.class)),

        // Number functions
        LENGTH(new TypeSignature(IntegerValue.class, StringValue.class)),
        INDEXOF(new TypeSignature(IntegerValue.class, StringValue.class, StringValue.class)),

        DATETOLONG(new TypeSignature(LongValue.class, DateValue.class)),

        SHORT(new TypeSignature(ShortValue.class, StringValue.class)),
        INTEGER(new TypeSignature(IntegerValue.class, StringValue.class)),
        LONG(new TypeSignature(LongValue.class, StringValue.class)),
        FLOAT(new TypeSignature(FloatValue.class, StringValue.class)),
        DOUBLE(new TypeSignature(DoubleValue.class, StringValue.class)),

        ABS(new TypeSignature(NumberValue.class, NumberValue.class)),

        RANDOMINTEGER(new TypeSignature(IntegerValue.class)),
        RANDOMLONG(new TypeSignature(LongValue.class)),
        RANDOMFLOAT(new TypeSignature(FloatValue.class)),
        RANDOMDOUBLE(new TypeSignature(DoubleValue.class)),

        // Boolean functions
        RANDOMBOOLEAN(new TypeSignature(BooleanValue.class)),
        DEFINEDINROW(new TypeSignature(BooleanValue.class, GenericValue.class)),
        EVAL(new TypeSignature(BooleanValue.class, StringValue.class));

        private final TypeSignature typeSignature;

        FunctionType(final TypeSignature typeSignature) {
            this.typeSignature = typeSignature;
        }

        private TypeSignature getTypeSignature() {
            return typeSignature;
        }

        public static Function getFunction(final String functionName, final List<GenericValue> exprList) {

            final FunctionType type;

            try {
                type = FunctionType.valueOf(functionName.toUpperCase());
            }
            catch (IllegalArgumentException e) {
                return null;
            }

            final Class<? extends GenericValue> returnType = type.getTypeSignature().getReturnType();

            if (HUtil.isParentClass(BooleanValue.class, returnType))
                return new BooleanFunction(type, exprList);
            else if (HUtil.isParentClass(StringValue.class, returnType))
                return new StringFunction(type, exprList);
            else if (HUtil.isParentClass(NumberValue.class, returnType))
                return new NumberFunction(type, exprList);
            else if (HUtil.isParentClass(DateValue.class, returnType))
                return new DateFunction(type, exprList);

            return null;
        }
    }

    private final FunctionType functionType;

    public Function(final FunctionType functionType, final List<GenericValue> exprs) {
        super(null, exprs);
        this.functionType = functionType;
    }

    protected FunctionType getFunctionType() {
        return this.functionType;
    }

    protected TypeSignature getTypeSignature() {
        return this.getFunctionType().getTypeSignature();
    }

    protected boolean isIntervalDate() {
        return this.getFunctionType() == FunctionType.DATEINTERVAL;
    }

    protected boolean isConstantDate() {
        return this.getFunctionType() == FunctionType.DATECONSTANT;
    }

    protected void checkForNull(final String... vals) throws HBqlException {
        for (final Object val : vals) {
            if (val == null)
                throw new HBqlException("Null value in " + this.asString());
        }
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        int i = 0;
        if (this.getArgList().size() != this.getTypeSignature().getArgCount())
            throw new TypeException("Incorrect number of arguments in function " + this.getFunctionType().name()
                                    + " in " + this.asString());

        for (final Class<? extends GenericValue> clazz : this.getTypeSignature().getArgs()) {
            final Class<? extends GenericValue> type = this.getArg(i).validateTypes(this, false);
            try {
                this.validateParentClass(clazz, type);
            }
            catch (TypeException e) {
                // Catch the exception and improve message
                throw new TypeException("Invalid type " + type.getSimpleName() + " for arg " + i + " in function "
                                        + this.getFunctionName() + " in "
                                        + this.asString() + ".  Expecting type " + clazz.getSimpleName() + ".");
            }
            i++;
        }

        return this.getTypeSignature().getReturnType();
    }

    protected String getFunctionName() {
        return this.getFunctionType().name();
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        if (!this.isAConstant())
            return this;
        else
            try {
                return this.getFunctionType().getTypeSignature().newLiteral(this.getValue(null));
            }
            catch (ResultMissingColumnException e) {
                throw new InternalErrorException();
            }
    }

    public String asString() {
        return this.getFunctionName() + super.asString();
    }
}