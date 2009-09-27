package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericStringPatternStmt extends GenericNotValue {

    private GenericValue valueExpr = null;
    private GenericValue patternExpr = null;

    protected GenericStringPatternStmt(final GenericValue valueExpr, final boolean not, final GenericValue patternExpr) {
        super(not);
        this.valueExpr = valueExpr;
        this.patternExpr = patternExpr;
    }

    protected GenericValue getValueExpr() {
        return this.valueExpr;
    }

    protected GenericValue getPatternExpr() {
        return this.patternExpr;
    }

    protected abstract String getFunctionName();

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        HUtil.validateParentClass(this,
                                  StringValue.class,
                                  this.getValueExpr().validateTypes(this, false),
                                  this.getPatternExpr().validateTypes(this, false));
        return BooleanValue.class;
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.valueExpr = this.getValueExpr().getOptimizedValue();
        this.patternExpr = this.getPatternExpr().getOptimizedValue();
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public boolean isAConstant() throws HBqlException {
        return this.getValueExpr().isAConstant() && this.getPatternExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getValueExpr().setContext(context);
        this.getPatternExpr().setContext(context);
    }

    @Override
    public String asString() {
        return this.getValueExpr().asString() + notAsString()
               + " " + this.getFunctionName() + " " + this.getPatternExpr().asString();
    }

}