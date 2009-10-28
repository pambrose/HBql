package org.apache.expreval.expr;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.client.TypeException;
import org.apache.expreval.expr.literal.DoubleLiteral;
import org.apache.expreval.expr.literal.FloatLiteral;
import org.apache.expreval.expr.literal.IntegerLiteral;
import org.apache.expreval.expr.literal.LongLiteral;
import org.apache.expreval.expr.literal.ShortLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.expreval.util.Lists;

import java.util.Arrays;
import java.util.List;

public abstract class GenericExpression implements GenericValue {

    // These are used to cache type of the args for exprs with numberic args
    private Class<? extends GenericValue> highestRankingNumericArgFoundInValidate = NumberValue.class;
    private Class rankingClass = null;
    private boolean useDecimal = false;
    private boolean useShort = false;
    private boolean useInteger = false;
    private boolean useLong = false;
    private boolean useFloat = false;
    private boolean useDouble = false;

    private ExpressionContext expressionContext = null;
    private final ExpressionType type;
    private final List<GenericValue> exprList = Lists.newArrayList();

    protected GenericExpression(final ExpressionType type, final GenericValue... exprs) {
        this(type, Arrays.asList(exprs));
    }

    protected GenericExpression(final ExpressionType type, final List<GenericValue> exprList) {
        this.type = type;
        if (exprList != null)
            this.getArgList().addAll(exprList);
    }

    protected GenericExpression(final ExpressionType type, final GenericValue expr, final List<GenericValue> exprList) {
        this.type = type;
        this.getArgList().add(expr);
        if (exprList != null)
            this.getArgList().addAll(exprList);
    }

    protected TypeSignature getTypeSignature() {
        return this.type.getTypeSignature();
    }

    protected List<GenericValue> getArgList() {
        return this.exprList;
    }

    protected List<GenericValue> getSubArgs(final int i) {
        return this.getArgList().subList(i, this.getArgList().size());
    }

    private Class<? extends GenericValue> getHighestRankingNumericArgFoundInValidate() {
        return this.highestRankingNumericArgFoundInValidate;
    }

    // These require getHighestRankingNumericArg() be called first to set value
    protected boolean useDecimal() {
        return this.useDecimal;
    }

    protected Number getValueWithCast(final long result) throws HBqlException {
        if (this.useShort)
            return (short)result;
        else if (this.useInteger)
            return (int)result;
        else if (this.useLong)
            return result;
        else
            throw new HBqlException("Invalid class: " + rankingClass.getName());
    }

    protected Number getValueWithCast(final double result) throws HBqlException {
        if (this.useFloat)
            return (float)result;
        else if (this.useDouble)
            return result;
        else
            throw new HBqlException("Invalid class: " + rankingClass.getName());
    }

    protected Class validateNumericArgTypes(final Object... objs) {

        if (rankingClass == null) {

            // If we do not already know the specific types, then look at the class of both args
            if (this.getHighestRankingNumericArgFoundInValidate() == NumberValue.class)
                this.rankingClass = NumericType.getHighestRankingNumericArg(objs);
            else
                this.rankingClass = this.getHighestRankingNumericArgFoundInValidate();

            this.useDecimal = NumericType.useDecimalNumericArgs(rankingClass);

            this.useShort = NumericType.isAShort(rankingClass);
            this.useInteger = NumericType.isAnInteger(rankingClass);
            this.useLong = NumericType.isALong(rankingClass);
            this.useFloat = NumericType.isAFloat(rankingClass);
            this.useDouble = NumericType.isADouble(rankingClass);
        }

        return this.rankingClass;
    }

    public boolean isAConstant() {
        for (final GenericValue val : this.getArgList())
            if (!val.isAConstant())
                return false;
        return true;
    }

    public boolean isDefaultKeyword() {
        return false;
    }

    public boolean hasAColumnReference() {
        for (final GenericValue val : this.getArgList())
            if (val.hasAColumnReference())
                return true;
        return false;
    }

    public void reset() {
        for (final GenericValue val : this.getArgList())
            val.reset();
    }

    public void setExprContext(final ExpressionContext context) throws HBqlException {

        this.expressionContext = context;

        for (final GenericValue val : this.getArgList())
            val.setExprContext(context);
    }

    protected ExpressionContext getExprContext() {
        return this.expressionContext;
    }

    public void optimizeArgs() throws HBqlException {
        // TODO May want to keep track if this has already been called
        for (int i = 0; i < this.getArgList().size(); i++)
            this.setArg(i, this.getArg(i).getOptimizedValue());
    }

    public GenericValue getArg(final int i) {
        return this.getArgList().get(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getArgList().set(i, val);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        if (this.getArgList().size() != this.getTypeSignature().getArgCount())
            throw new TypeException("Incorrect number of arguments in " + this.asString());

        for (int i = 0; i < this.getTypeSignature().getArgCount(); i++)
            this.validateParentClass(this.getTypeSignature().getArg(i), this.getArg(i).validateTypes(this, false));

        return this.getTypeSignature().getReturnType();
    }

    protected Class<? extends GenericValue> validateNumericTypes() throws HBqlException {

        if (this.getArgList().size() != this.getTypeSignature().getArgCount())
            throw new TypeException("Incorrect number of arguments in " + this.asString());

        // Return the type of the highest ranking numeric arg
        int highestRank = -1;
        for (int i = 0; i < this.getTypeSignature().getArgCount(); i++) {

            final Class<? extends GenericValue> clazz = this.getArg(i).validateTypes(this, false);
            this.validateParentClass(this.getTypeSignature().getArg(i), clazz);

            final int rank = NumericType.getTypeRanking(clazz);
            if (rank > highestRank) {
                highestRank = rank;
                this.highestRankingNumericArgFoundInValidate = clazz;
            }
        }

        return this.getHighestRankingNumericArgFoundInValidate();
    }

    public GenericValue getOptimizedValue() throws HBqlException {

        this.optimizeArgs();

        if (!this.isAConstant())
            return this;

        try {
            final Object obj = this.getValue(null);

            if (this.getTypeSignature().getReturnType() == BooleanValue.class
                || this.getTypeSignature().getReturnType() == StringValue.class
                || this.getTypeSignature().getReturnType() == DateValue.class)
                return this.getTypeSignature().newLiteral(obj);

            if (Util.isParentClass(NumberValue.class, this.getTypeSignature().getReturnType())) {

                if (obj instanceof Short)
                    return new ShortLiteral((Short)obj);

                if (obj instanceof Integer)
                    return new IntegerLiteral((Integer)obj);

                if (obj instanceof Long)
                    return new LongLiteral((Long)obj);

                if (obj instanceof Float)
                    return new FloatLiteral((Float)obj);

                if (obj instanceof Double)
                    return new DoubleLiteral((Double)obj);
            }
            throw new InternalErrorException(this.getTypeSignature().getReturnType().getSimpleName());
        }
        catch (ResultMissingColumnException e) {
            // This will never be hit because the exception is for constants only
            throw new InternalErrorException();
        }
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder("(");

        boolean first = true;
        for (final GenericValue val : this.getArgList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(val.asString());
            first = false;
        }

        sbuf.append(")");

        return sbuf.toString();
    }

    public void validateParentClass(final Class<? extends GenericValue> parentClazz,
                                    final Class<? extends GenericValue>... clazzes) throws TypeException {

        List<Class<? extends GenericValue>> classList = Lists.newArrayList();

        for (final Class<? extends GenericValue> clazz : clazzes) {

            if (clazz == null)
                continue;

            if (Util.isParentClass(NumberValue.class, parentClazz)) {
                if (!Util.isParentClass(NumberValue.class, clazz)) {
                    classList.add(clazz);
                }
                else {
                    if (!NumericType.isAssignable(parentClazz, clazz))
                        classList.add(clazz);
                }
            }
            else {
                if (!parentClazz.isAssignableFrom(clazz))
                    classList.add(clazz);
            }
        }

        if (classList.size() > 0) {
            final StringBuilder sbuf = new StringBuilder("Expecting type " + parentClazz.getSimpleName()
                                                         + " but encountered type"
                                                         + ((classList.size() > 1) ? "s" : "") + " ");
            boolean first = true;
            for (final Class clazz : classList) {
                if (!first)
                    sbuf.append(", ");
                sbuf.append(clazz.getSimpleName());
                first = false;
            }

            sbuf.append(" in expression: " + this.asString());

            throw new TypeException(sbuf.toString());
        }
    }

    public void throwInvalidTypeException(final Class<? extends GenericValue>... clazzes) throws TypeException {

        final List<Class> classList = Lists.newArrayList();

        for (final Class clazz : clazzes)
            if (clazz != null)
                classList.add(clazz);

        final StringBuilder sbuf = new StringBuilder("Invalid type");
        sbuf.append(((classList.size() > 1) ? "s " : " "));

        boolean first = true;
        for (final Class<? extends GenericValue> clazz : clazzes) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(clazz.getSimpleName());
            first = false;
        }
        sbuf.append(" in expression " + this.asString());

        throw new TypeException(sbuf.toString());
    }

    protected Class<? extends GenericValue> determineGenericValueClass(final Class<? extends GenericValue> clazz) throws TypeException {

        final List<Class<? extends GenericValue>> types = Arrays.asList(StringValue.class,
                                                                        NumberValue.class,
                                                                        DateValue.class,
                                                                        BooleanValue.class);

        for (final Class<? extends GenericValue> type : types)
            if (Util.isParentClass(type, clazz))
                return type;

        this.throwInvalidTypeException(clazz);
        return null;
    }
}
