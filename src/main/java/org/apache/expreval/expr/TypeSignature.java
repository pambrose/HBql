package org.apache.expreval.expr;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.expr.literal.DoubleLiteral;
import org.apache.expreval.expr.literal.FloatLiteral;
import org.apache.expreval.expr.literal.IntegerLiteral;
import org.apache.expreval.expr.literal.LongLiteral;
import org.apache.expreval.expr.literal.ShortLiteral;
import org.apache.expreval.expr.literal.StringLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.FloatValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ShortValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.expreval.util.Lists;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TypeSignature implements Serializable {

    private final Class<? extends GenericValue> returnType;
    private final List<Class<? extends GenericValue>> typeSig = Lists.newArrayList();
    private final transient Constructor literalConstructor;
    private final Class literalCastClass;

    public TypeSignature(final Class<? extends GenericValue> returnType, Class<? extends GenericValue>... typeSig) {
        this.returnType = returnType;
        this.literalCastClass = this.getLiteralCastClass(this.getReturnType());
        this.literalConstructor = this.getLiteralConstructor(this.getReturnType(), this.getLiteralCastClass());
        this.typeSig.addAll(Arrays.asList(typeSig));
    }

    private Class getLiteralCastClass(Class<? extends GenericValue> type) {

        if (type == null)
            return null;
        else if (type == BooleanValue.class)
            return Boolean.class;
        else if (type == StringValue.class)
            return String.class;
        else if (type == DateValue.class)
            return Long.class;    // Note this is Long and not Date
        else if (type == ShortValue.class)
            return Short.class;
        else if (type == IntegerValue.class)
            return Integer.class;
        else if (type == LongValue.class)
            return Long.class;
        else if (type == FloatValue.class)
            return Float.class;
        else if (type == DoubleValue.class)
            return Double.class;
        else if (type == NumberValue.class)
            return Number.class;
        else
            throw new RuntimeException("Invalid return type in signature: " + type.getName());
    }

    private Constructor getLiteralConstructor(Class type, final Class literalCastClass) {

        final Class clazz;

        try {
            if (type == null)
                return null;
            else if (type == BooleanValue.class || type == Boolean.class)
                clazz = BooleanLiteral.class;
            else if (type == StringValue.class || type == String.class)
                clazz = StringLiteral.class;
            else if (type == DateValue.class || type == Date.class)
                clazz = DateLiteral.class;
            else if (type == ShortValue.class || type == Short.class)
                clazz = ShortLiteral.class;
            else if (type == IntegerValue.class || type == Integer.class)
                clazz = IntegerLiteral.class;
            else if (type == LongValue.class || type == Long.class)
                clazz = LongLiteral.class;
            else if (type == FloatValue.class || type == Float.class)
                clazz = FloatLiteral.class;
            else if (type == DoubleValue.class || type == Double.class)
                clazz = DoubleLiteral.class;
            else if (type == NumberValue.class || type == Number.class)
                return null;
            else
                throw new RuntimeException("Invalid return type in signature: " + type.getName());

            return clazz.getConstructor(literalCastClass);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Invalid literal constructor in signature: " + type.getName());
        }
    }

    public GenericValue newLiteral(final Object val) throws HBqlException {
        try {

            final Constructor constructor;
            final Object castedObject;

            if (this.getReturnType() == NumberValue.class) {
                constructor = this.getLiteralConstructor(val.getClass(), val.getClass());
                castedObject = val;
            }
            else {
                constructor = this.getLiteralConstructor();
                castedObject = this.getLiteralCastClass().cast(val);
            }

            return (GenericValue)constructor.newInstance(castedObject);
        }
        catch (InstantiationException e) {
            throw new InternalErrorException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new InternalErrorException(e.getMessage());
        }
        catch (InvocationTargetException e) {
            throw new InternalErrorException(e.getMessage());
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
