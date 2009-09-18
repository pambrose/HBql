package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 6:20:20 PM
 */
public class StringConcat implements StringValue {

    private StringValue val1, val2;

    public StringConcat(final StringValue val1, StringValue val2) {
        this.val1 = val1;
        this.val2 = val2;
    }


    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = Lists.newArrayList();
        retval.addAll(val1.getExprVariables());
        retval.addAll(val2.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    @Override
    public String getCurrentValue(final Object object) throws HPersistException {

        return this.val1.getCurrentValue(object) + this.val2.getCurrentValue(object);
    }

    @Override
    public boolean isAConstant() {
        return this.val1.isAConstant() && this.val2.isAConstant();
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.val1.optimizeForConstants(object))
            this.val1 = new StringLiteral(this.val1.getCurrentValue(object));
        else
            retval = false;

        if (this.val2.optimizeForConstants(object))
            this.val2 = new StringLiteral(this.val2.getCurrentValue(object));
        else
            retval = false;

        return retval;

    }

    @Override
    public void setSchema(final Schema schema) {
        this.val1.setSchema(schema);
        this.val2.setSchema(schema);
    }

}
