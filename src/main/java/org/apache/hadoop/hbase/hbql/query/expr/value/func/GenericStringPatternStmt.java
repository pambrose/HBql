package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericStringPatternStmt extends GenericNotValue {

    protected GenericStringPatternStmt(final GenericValue arg0, final boolean not, final GenericValue arg1) {
        super(new TypeSignature(BooleanValue.class, StringValue.class, StringValue.class),
              not,
              arg0,
              arg1);
    }

    protected abstract String getFunctionName();

    @Override
    public String asString() {
        return this.getArg(0).asString() + notAsString()
               + " " + this.getFunctionName() + " " + this.getArg(1).asString();
    }

}