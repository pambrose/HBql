package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.query.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.IntegerValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.LongValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ShortValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 2, 2009
 * Time: 9:56:07 PM
 */
public enum NumericType {

    ShortType(ShortValue.class),
    IntegerType(IntegerValue.class),
    LongType(LongValue.class),
    FloatType(FloatValue.class),
    DoubleType(DoubleValue.class),
    NumberType(NumberValue.class);

    final Class<? extends GenericValue> exprType;

    private NumericType(final Class<? extends GenericValue> exprType) {
        this.exprType = exprType;
    }

    private Class<? extends GenericValue> getExprType() {
        return this.exprType;
    }

    public static int getTypeRanking(final Class clazz) {
        for (final NumericType type : values())
            if (clazz.equals(type.getExprType()))
                return type.ordinal();
        return -1;
    }
}
