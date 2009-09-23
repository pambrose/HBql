package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.Arrays;
import java.util.List;

public enum FunctionType {
    // Return Strings
    TRIM(StringValue.class, (List)Arrays.asList(StringValue.class)),
    LOWER(StringValue.class, (List)Arrays.asList(StringValue.class)),
    UPPER(StringValue.class, (List)Arrays.asList(StringValue.class)),
    CONCAT(StringValue.class, (List)Arrays.asList(StringValue.class, StringValue.class)),
    REPLACE(StringValue.class, (List)Arrays.asList(StringValue.class, StringValue.class, StringValue.class)),

    // Return Booleans
    CONTAINS(BooleanValue.class, (List)Arrays.asList(StringValue.class, StringValue.class)),

    // Return Numbers
    LENGTH(NumberValue.class, (List)Arrays.asList(StringValue.class)),
    INDEXOF(NumberValue.class, (List)Arrays.asList(StringValue.class, StringValue.class));

    private final Class<? extends ValueExpr> returnType;
    private final List<Class> typeSig;

    FunctionType(final Class<? extends ValueExpr> returnType, final List<Class> typeSig) {
        this.returnType = returnType;
        this.typeSig = typeSig;
    }


    public Class<? extends ValueExpr> getReturnType() {
        return returnType;
    }

    private List<Class> getTypeSig() {
        return typeSig;
    }


    public void validateArgs(final ValueExpr[] valueExprs) throws HPersistException {

        int i = 0;

        if (valueExprs.length != this.getTypeSig().size())
            throw new HPersistException("Incorrect number of arguments in function " + this.name());

        for (final Class clazz : this.getTypeSig()) {

            final Class type = valueExprs[i].validateType();

            if (!clazz.equals(type))
                throw new HPersistException("Invalid type " + type.getName() + " for arg " + i + " in function "
                                            + this.name() + ".  Expecting type " + clazz.getName());
            i++;
        }

    }
}