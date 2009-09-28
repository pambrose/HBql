package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 27, 2009
 * Time: 9:40:01 PM
 */
public class TypeSignature {

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

        BOOLEANEXPR(new TypeSignature(BooleanValue.class, BooleanValue.class));

        private final TypeSignature typeSignature;

        Type(final TypeSignature typeSignature) {
            this.typeSignature = typeSignature;
        }

        public TypeSignature getTypeSignature() {
            return typeSignature;
        }
    }

    private final Class<? extends GenericValue> returnType;
    private final List<Class<? extends GenericValue>> typeSig;

    public TypeSignature(final Class<? extends GenericValue> returnType,
                         Class<? extends GenericValue>... typeSig) {
        this.returnType = returnType;
        this.typeSig = Lists.newArrayList();
        for (final Class<? extends GenericValue> sig : typeSig)
            this.typeSig.add(sig);
    }

    public TypeSignature(final Class<? extends GenericValue> returnType,
                         final List<Class<? extends GenericValue>> typeSig) {
        this.returnType = returnType;
        this.typeSig = typeSig;
    }

    public Class<? extends GenericValue> getReturnType() {
        return returnType;
    }

    public List<Class<? extends GenericValue>> getArgs() {
        return typeSig;
    }

    public Class<? extends GenericValue> getArg(final int i) {
        return this.getArgs().get(i);
    }

    public int getArgCount() {
        return this.getArgs().size();
    }

}
