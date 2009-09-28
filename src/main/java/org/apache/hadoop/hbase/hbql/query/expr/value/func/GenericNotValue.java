package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericNotValue extends GenericExpr implements BooleanValue {

    private final boolean not;

    protected GenericNotValue(final TypeSignature typeSignature, final boolean not, GenericValue... args) {
        super(typeSignature, args);
        this.not = not;
    }

    protected GenericNotValue(final TypeSignature typeSignature, final boolean not, final List<GenericValue> args) {
        super(typeSignature, args);
        this.not = not;
    }

    protected GenericNotValue(final TypeSignature typeSignature, final boolean not, final GenericValue arg, final List<GenericValue> argList) {
        super(typeSignature, arg, argList);
        this.not = not;
    }

    public boolean isNot() {
        return not;
    }

    protected String notAsString() {
        return (this.isNot()) ? " NOT" : "";
    }

}