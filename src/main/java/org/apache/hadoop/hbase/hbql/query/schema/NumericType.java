package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.query.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.IntegerValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.LongValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ShortValue;

public enum NumericType {

    ShortType(ShortValue.class, Short.class),
    IntegerType(IntegerValue.class, Integer.class),
    LongType(LongValue.class, Long.class),
    FloatType(FloatValue.class, Float.class),
    DoubleType(DoubleValue.class, Double.class),
    NumberType(NumberValue.class, Number.class);

    final Class<? extends GenericValue> exprType;
    final Class<? extends Number> primType;

    private NumericType(final Class<? extends GenericValue> exprType, final Class<? extends Number> primType) {
        this.exprType = exprType;
        this.primType = primType;
    }

    private Class<? extends GenericValue> getExprType() {
        return this.exprType;
    }

    private Class<? extends Number> getPrimType() {
        return primType;
    }

    public static int getTypeRanking(final Class clazz) {
        for (final NumericType type : values())
            if (clazz.equals(type.getExprType()) || clazz.equals(type.getPrimType()))
                return type.ordinal();
        return -1;
    }

    public static boolean isAssignable(final Class parentClazz, final Class lowerClazz) {
        final int parentRanking = getTypeRanking(parentClazz);
        final int clazzRanking = getTypeRanking(lowerClazz);

        return clazzRanking <= parentRanking;
    }

    public static Class getHighestRankingNumericArg(final Object... vals) {

        Class highestRankingNumericArg = NumberValue.class;
        int highestRank = -1;
        for (final Object obj : vals) {

            final Class clazz = obj.getClass();
            final int rank = NumericType.getTypeRanking(clazz);
            if (rank > highestRank) {
                highestRank = rank;
                highestRankingNumericArg = clazz;
            }
        }
        return highestRankingNumericArg;
    }

    public static boolean useDecimalNumericArgs(final Class clazz) {
        return isAFloat(clazz) || isADouble(clazz);
    }

    public static boolean isANumber(final Class clazz) {
        return getTypeRanking(clazz) != -1;
    }

    public static boolean isAShort(final Class clazz) {
        return clazz.equals(ShortValue.class) || clazz.equals(Short.class);
    }

    public static boolean isAnInteger(final Class clazz) {
        return clazz.equals(IntegerValue.class) || clazz.equals(Integer.class);
    }

    public static boolean isALong(final Class clazz) {
        return clazz.equals(LongValue.class) || clazz.equals(Long.class);
    }

    public static boolean isAFloat(final Class clazz) {
        return clazz.equals(FloatValue.class) || clazz.equals(Float.class);
    }

    public static boolean isADouble(final Class clazz) {
        return clazz.equals(DoubleValue.class) || clazz.equals(Double.class);
    }
}
