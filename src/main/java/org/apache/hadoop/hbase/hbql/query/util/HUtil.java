package org.apache.hadoop.hbase.hbql.query.util;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.io.Serialization;
import org.apache.hadoop.hbase.hbql.query.schema.NumericType;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Collection;

public class HUtil {

    private final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static Serialization getSerialization() {
        return ser;
    }

    public static String getZeroPaddedNumber(final long val, final int width) throws HBqlException {

        final String strval = "" + val;
        final int padsize = width - strval.length();
        if (padsize < 0)
            throw new HBqlException("Value " + val + " exceeds width " + width);

        StringBuilder sbuf = new StringBuilder();
        for (int i = 0; i < padsize; i++)
            sbuf.append("0");

        sbuf.append(strval);
        return sbuf.toString();
    }

    public static void logException(final Log log, final Exception e) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintWriter oos = new PrintWriter(baos);

        e.printStackTrace(oos);
        oos.flush();
        oos.close();

        log.info(baos.toString());
    }

    public static boolean isACollection(final Object obj) {
        return HUtil.isParentClass(Collection.class, obj.getClass());
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

        if (HUtil.isParentClass(NumberValue.class, clazz))
            return NumberValue.class;

        if (HUtil.isParentClass(StringValue.class, clazz))
            return StringValue.class;

        if (HUtil.isParentClass(DateValue.class, clazz))
            return DateValue.class;

        if (HUtil.isParentClass(BooleanValue.class, clazz))
            return BooleanValue.class;

        return null;
    }
}
