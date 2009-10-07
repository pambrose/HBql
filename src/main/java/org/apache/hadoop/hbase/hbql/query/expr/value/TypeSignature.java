package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class TypeSignature {

    private final Class<? extends GenericValue> returnType;
    private final List<Class<? extends GenericValue>> typeSig;
    private final Constructor literalConstructor;
    private final Class literalCastClass;

    public TypeSignature(final Class<? extends GenericValue> returnType, Class<? extends GenericValue>... typeSig) {
        this.returnType = returnType;

        this.literalCastClass = getLiteralCastClass(this.getReturnType());
        this.literalConstructor = getLiteralConstructor(this.getReturnType());

        this.typeSig = Lists.newArrayList();
        for (final Class<? extends GenericValue> sig : typeSig)
            this.typeSig.add(sig);
    }

    private Class getLiteralCastClass(Class<? extends GenericValue> type) {

        if (type == null)
            return null;
        else if (type.equals(StringValue.class))
            return String.class;
        else if (type.equals(DateValue.class))
            return Long.class;    // Note this is Long and not Date
        else if (type.equals(BooleanValue.class))
            return Boolean.class;
        else if (type.equals(ShortValue.class))
            return Short.class;
        else if (type.equals(IntegerValue.class))
            return Integer.class;
        else if (type.equals(LongValue.class))
            return Long.class;
        else if (type.equals(FloatValue.class))
            return Float.class;
        else if (type.equals(DoubleValue.class))
            return Double.class;
        else if (type.equals(NumberValue.class))
            return Number.class;
        else
            throw new RuntimeException("Invalid return type in signature: " + type.getName());
    }

    private Constructor getLiteralConstructor(Class<? extends GenericValue> type) {

        try {
            if (type == null)
                return null;
            else if (type.equals(StringValue.class))
                return StringLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(DateValue.class))
                return DateLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(BooleanValue.class))
                return BooleanLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(ShortValue.class))
                return ShortLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(IntegerValue.class))
                return IntegerLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(LongValue.class))
                return LongLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(FloatValue.class))
                return FloatLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(DoubleValue.class))
                return DoubleLiteral.class.getConstructor(this.getLiteralCastClass());
            else if (type.equals(NumberValue.class))
                return null;
            else
                throw new RuntimeException("Invalid return type in signature: " + type.getName());
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Invalid literal constructor in signature: " + type.getName());
        }
    }

    public TypeSignature(final Class<? extends GenericValue> returnType,
                         final List<Class<? extends GenericValue>> typeSig) {
        this.returnType = returnType;
        this.literalCastClass = getLiteralCastClass(this.getReturnType());
        this.literalConstructor = getLiteralConstructor(this.getReturnType());
        this.typeSig = typeSig;
    }

    public GenericValue newLiteral(final Object val) throws HBqlException {
        try {
            return (GenericValue)this.getLiteralConstructor().newInstance(this.getLiteralCastClass().cast(val));
        }
        catch (InstantiationException e) {
            throw new HBqlException("Internal error: " + e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Internal error: " + e.getMessage());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Internal error: " + e.getMessage());
        }
    }

    public Class<? extends GenericValue> getReturnType() {
        return returnType;
    }

    public List<Class<? extends GenericValue>> getArgs() {
        return typeSig;
    }

    private Constructor getLiteralConstructor() {
        return this.literalConstructor;
    }

    private Class getLiteralCastClass() {
        return this.literalCastClass;
    }

    public Class<? extends GenericValue> getArg(final int i) {
        return this.getArgs().get(i);
    }

    public int getArgCount() {
        return this.getArgs().size();
    }
}
