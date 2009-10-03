package org.apache.hadoop.hbase.hbql.query.schema;

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
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 5:01:22 PM
 */
public enum FieldType {

    BooleanType(Boolean.TYPE, BooleanValue.class, 0, Bytes.SIZEOF_BOOLEAN, "BOOLEAN", "BOOL"),
    ByteType(Byte.TYPE, NumberValue.class, 1, Bytes.SIZEOF_BYTE, "BYTE"),
    CharType(Short.TYPE, NumberValue.class, 1, Bytes.SIZEOF_CHAR, "CHAR"),

    ShortType(Short.TYPE, ShortValue.class, 2, Bytes.SIZEOF_SHORT, "SHORT"),
    IntegerType(Integer.TYPE, IntegerValue.class, 3, Bytes.SIZEOF_INT, "INTEGER", "INT"),
    LongType(Long.TYPE, LongValue.class, 4, Bytes.SIZEOF_LONG, "LONG"),
    FloatType(Float.TYPE, FloatValue.class, 5, Bytes.SIZEOF_FLOAT, "FLOAT"),
    DoubleType(Double.TYPE, DoubleValue.class, 6, Bytes.SIZEOF_DOUBLE, "DOUBLE"),

    KeyType(String.class, StringValue.class, -1, -1, "KEY"),
    StringType(String.class, StringValue.class, -1, -1, "STRING", "STRING", "VARCHAR"),
    DateType(Date.class, DateValue.class, -1, -1, "DATE", "DATETIME"),
    ObjectType(Object.class, null, -1, -1, "OBJECT", "OBJ");

    private final Class clazz;
    private Class<? extends GenericValue> exprType;
    private final int typeRanking;
    private final int size;
    private final List<String> synonymList;


    FieldType(final Class clazz,
              final Class<? extends GenericValue> exprType,
              final int typeRanking,
              final int size,
              final String... synonyms) {
        this.clazz = clazz;
        this.exprType = exprType;
        this.typeRanking = typeRanking;
        this.size = size;
        this.synonymList = Lists.newArrayList();
        this.synonymList.addAll(Arrays.asList(synonyms));
    }

    public Class getClazz() {
        return this.clazz;
    }

    public int getTypeRanking() {
        return this.typeRanking;
    }

    public int getSize() {
        return this.size;
    }

    public Class<? extends GenericValue> getExprType() {
        return this.exprType;
    }

    public static FieldType getFieldType(final Object obj) {
        final Class fieldClass = obj.getClass();
        return getFieldType(fieldClass);
    }

    public static FieldType getFieldType(final Field field) {
        final Class fieldClass = field.getType();
        return getFieldType(fieldClass);
    }

    public String getFirstSynonym() {
        return this.getSynonymList().get(0);
    }

    private List<String> getSynonymList() {
        return this.synonymList;
    }

    public static int getTypeRanking(final Class clazz) {
        for (final FieldType type : values())
            if (clazz.equals(type.getExprType()))
                return type.getTypeRanking();
        return -1;
    }

    public static FieldType getFieldType(final Class fieldClass) {

        final Class<?> clazz = fieldClass.isArray() ? fieldClass.getComponentType() : fieldClass;

        if (!clazz.isPrimitive()) {
            if (clazz.equals(String.class))
                return StringType;
            else if (clazz.equals(Date.class))
                return DateType;
            else
                return ObjectType;
        }
        else {
            for (final FieldType type : values())
                if (clazz.equals(type.getClazz()))
                    return type;
        }

        throw new RuntimeException("Unknown type: " + clazz + " in FieldType.getFieldType()");
    }

    public static FieldType getFieldType(final String desc) throws HBqlException {

        for (final FieldType type : values()) {
            if (type.matchesSynonym(desc))
                return type;
        }

        throw new HBqlException("Unknown type description: " + desc);
    }

    private boolean matchesSynonym(final String str) {
        for (final String syn : this.getSynonymList())
            if (str.equalsIgnoreCase(syn))
                return true;
        return false;
    }
}
