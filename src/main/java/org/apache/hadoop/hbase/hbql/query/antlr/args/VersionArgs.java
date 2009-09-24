package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class VersionArgs {

    private final ValueExpr value;

    public VersionArgs(final ValueExpr val) {
        this.value = val;
    }

    public boolean isValid() {
        return this.value != null;
    }

    public int getValue() throws HPersistException {

        if (this.value == null)
            throw new HPersistException("Null value invalid in VersionArgs");

        final Class clazz = this.value.getClass();
        if (!clazz.equals(NumberValue.class))
            throw new HPersistException("Invalid type " + clazz.getName() + " in VersionArgs");

        return ((Number)this.value.getValue(null)).intValue();
    }
}