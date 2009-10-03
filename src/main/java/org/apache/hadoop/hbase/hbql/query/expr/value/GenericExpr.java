package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.IntegerValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.LongValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ShortValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DoubleLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.FloatLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.IntegerLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.LongLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.ShortLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public abstract class GenericExpr implements GenericValue {

    public enum Type {

        BOOLEANTERNARY(new TypeSignature(BooleanValue.class, BooleanValue.class, BooleanValue.class, BooleanValue.class)),
        STRINGTERNARY(new TypeSignature(StringValue.class, BooleanValue.class, StringValue.class, StringValue.class)),
        DATETERNARY(new TypeSignature(DateValue.class, BooleanValue.class, DateValue.class, DateValue.class)),
        NUMBERTERNARY(new TypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class, NumberValue.class)),

        STRINGCALCULATION(new TypeSignature(StringValue.class, StringValue.class, StringValue.class)),
        DATECALCULATION(new TypeSignature(DateValue.class, DateValue.class, DateValue.class)),
        NUMBERCALCULATION(new TypeSignature(NumberValue.class, NumberValue.class, NumberValue.class)),

        STRINGBETWEEN(new TypeSignature(BooleanValue.class, StringValue.class, StringValue.class, StringValue.class)),
        DATEBETWEEN(new TypeSignature(BooleanValue.class, DateValue.class, DateValue.class, DateValue.class)),
        NUMBERBETWEEN(new TypeSignature(BooleanValue.class, NumberValue.class, NumberValue.class, NumberValue.class)),

        STRINGNULL(new TypeSignature(BooleanValue.class, StringValue.class)),

        STRINGPATTERN(new TypeSignature(BooleanValue.class, StringValue.class, StringValue.class)),

        DATESTRING(new TypeSignature(DateValue.class, StringValue.class, StringValue.class)),

        INTERVAL(new TypeSignature(DateValue.class, NumberValue.class)),

        BOOLEANEXPR(new TypeSignature(BooleanValue.class, BooleanValue.class)),

        // Args are left unspecified for IN Stmt
        GENERICINSTMT(new TypeSignature(BooleanValue.class));

        private final TypeSignature typeSignature;

        Type(final TypeSignature typeSignature) {
            this.typeSignature = typeSignature;
        }

        private TypeSignature getTypeSignature() {
            return typeSignature;
        }
    }

    private final Type type;
    private final List<GenericValue> argList = Lists.newArrayList();

    protected GenericExpr(final Type type, final GenericValue... exprs) {
        this(type, Arrays.asList(exprs));
    }

    protected GenericExpr(final Type type, final List<GenericValue> exprList) {
        this.type = type;
        this.argList.addAll(exprList);
    }

    protected GenericExpr(final Type type, final GenericValue expr, final List<GenericValue> exprList) {
        this.type = type;
        this.argList.add(expr);
        this.argList.addAll(exprList);
    }

    protected TypeSignature getTypeSignature() {
        return this.type.getTypeSignature();
    }

    protected List<GenericValue> getArgList() {
        return this.argList;
    }

    protected List<GenericValue> getSubArgs(final int i) {
        return this.getArgList().subList(i, this.getArgList().size());
    }

    public boolean isAConstant() {
        for (final GenericValue val : this.getArgList())
            if (!val.isAConstant())
                return false;
        return true;
    }

    public void setExprContext(final ExprContext context) throws HBqlException {
        for (final GenericValue val : this.getArgList())
            val.setExprContext(context);
    }

    public void optimizeArgs() throws HBqlException {
        for (int i = 0; i < this.getArgList().size(); i++)
            this.setArg(i, this.getArg(i).getOptimizedValue());
    }

    public GenericValue getArg(final int i) {
        return this.getArgList().get(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getArgList().set(i, val);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        if (this.getArgList().size() != this.getTypeSignature().getArgCount())
            throw new TypeException("Incorrect number of variables in " + this.asString());

        for (int i = 0; i < this.getTypeSignature().getArgCount(); i++)
            this.validateParentClass(this.getTypeSignature().getArg(i), this.getArg(i).validateTypes(this, false));

        return this.getTypeSignature().getReturnType();
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        this.optimizeArgs();

        if (this.getTypeSignature().getReturnType().equals(BooleanValue.class))
            return this.isAConstant() ? new BooleanLiteral((Boolean)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(StringValue.class))
            return this.isAConstant() ? new StringLiteral((String)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(DateValue.class))
            return this.isAConstant() ? new DateLiteral((Long)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(ShortValue.class))
            return this.isAConstant() ? new ShortLiteral((Short)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(IntegerValue.class))
            return this.isAConstant() ? new IntegerLiteral((Integer)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(LongValue.class))
            return this.isAConstant() ? new LongLiteral((Long)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(FloatValue.class))
            return this.isAConstant() ? new FloatLiteral((Float)this.getValue(null)) : this;

        if (this.getTypeSignature().getReturnType().equals(DoubleValue.class))
            return this.isAConstant() ? new DoubleLiteral((Double)this.getValue(null)) : this;

        throw new HBqlException("Internal error");
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

        List<Class<? extends GenericValue>> classList = null;

        for (final Class<? extends GenericValue> clazz : clazzes) {

            if (clazz == null)
                continue;

            if (!parentClazz.isAssignableFrom(clazz)) {
                if (classList == null)
                    classList = Lists.newArrayList();
                classList.add(clazz);
            }
        }

        if (classList != null) {
            final StringBuilder sbuf = new StringBuilder("Expecting type " + parentClazz.getSimpleName()
                                                         + " but encountered type"
                                                         + ((classList.size() > 1) ? "s" : "") + ": ");
            boolean first = true;
            for (final Class clazz : classList) {
                if (!first)
                    sbuf.append(", ");
                sbuf.append(clazz.getSimpleName());
                first = false;
            }

            sbuf.append(" in expression " + this.asString());

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

}
