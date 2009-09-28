package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 27, 2009
 * Time: 9:40:01 PM
 */
public class TypeSignature {

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
