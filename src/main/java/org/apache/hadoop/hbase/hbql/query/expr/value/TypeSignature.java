package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

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
                         final List<Class<? extends GenericValue>> typeSig) {
        this.returnType = returnType;
        this.typeSig = typeSig;
    }

    public Class<? extends GenericValue> getSignatureReturnType() {
        return returnType;
    }

    public List<Class<? extends GenericValue>> getSignatureArgs() {
        return typeSig;
    }

    public int size() {
        return this.getSignatureArgs().size();
    }
}
