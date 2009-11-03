package org.apache.expreval.expr;

import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.ByteValue;
import org.apache.expreval.expr.node.CharValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.MapValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.expreval.expr.node.StringValue;

import java.util.Collection;

public class TypeSupport {

    public static boolean isACollection(final Object obj) {
        return isParentClass(Collection.class, obj.getClass());
    }

    public static boolean isParentClass(final Class parentClazz, final Class... clazzes) {

        final boolean parentIsANumber = NumericType.isANumber(parentClazz);

        for (final Class clazz : clazzes) {

            if (clazz == null)
                continue;

            if (parentIsANumber && NumericType.isANumber(clazz)) {
                if (!NumericType.isAssignable(parentClazz, clazz))
                    return false;
            }
            else {
                if (!parentClazz.isAssignableFrom(clazz))
                    return false;
            }
        }
        return true;
    }

    public static Class<? extends GenericValue> getGenericExprType(final GenericValue val) {

        final Class clazz = val.getClass();

        if (isParentClass(BooleanValue.class, clazz))
            return BooleanValue.class;

        if (isParentClass(ByteValue.class, clazz))
            return ByteValue.class;

        if (isParentClass(CharValue.class, clazz))
            return CharValue.class;

        if (isParentClass(NumberValue.class, clazz))
            return NumberValue.class;

        if (isParentClass(StringValue.class, clazz))
            return StringValue.class;

        if (isParentClass(DateValue.class, clazz))
            return DateValue.class;

        if (isParentClass(MapValue.class, clazz))
            return MapValue.class;

        if (isParentClass(ObjectValue.class, clazz))
            return ObjectValue.class;

        return null;
    }
}
