package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.NamedParameter;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.NumericType;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class ExprContext implements Serializable {

    private boolean inNeedOfTypeValidation = true;
    private boolean inNeedOfOptimization = true;
    private boolean inNeedOfSettingContext = true;

    private final List<GenericColumn> columnsUsedInExpr = Lists.newArrayList();
    private final List<ColumnAttrib> attribsUsedInExpr = Lists.newArrayList();
    private final Map<String, List<NamedParameter>> namedParamMap = Maps.newHashMap();

    private Schema schema = null;
    private final TypeSignature typeSignature;
    private final List<GenericValue> expressions = Lists.newArrayList();

    protected ExprContext(final TypeSignature typeSignature, final GenericValue... vals) {
        this.typeSignature = typeSignature;
        if (vals != null) {
            for (final GenericValue val : vals) {
                this.addExpression(val);
            }
        }
    }

    public abstract String asString();

    public abstract boolean useHBaseResult();

    public List<GenericColumn> getColumnsUsedInExpr() {
        return this.columnsUsedInExpr;
    }

    public List<ColumnAttrib> getAttribsUsedInExpr() {
        return this.attribsUsedInExpr;
    }

    public void addExpression(final GenericValue genericValue) {
        this.getExpressionList().add(genericValue);
    }

    public Map<String, List<NamedParameter>> getNamedParamMap() {
        return this.namedParamMap;
    }

    protected List<GenericValue> getExpressionList() {
        return this.expressions;
    }

    private TypeSignature getTypeSignature() {
        return this.typeSignature;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
        this.setContext();
    }

    protected GenericValue getGenericValue(final int i) {
        return this.getExpressionList().get(i);
    }

    protected Object evaluate(final int i,
                              final boolean allowColumns,
                              final boolean allowsCollections,
                              final Object object) throws HBqlException, ResultMissingColumnException {
        this.validateTypes(allowColumns, allowsCollections);
        this.optimize();
        return this.getGenericValue(i).getValue(object);
    }

    protected Object evaluateWithoutColumns(final int i,
                                            final boolean allowsCollections,
                                            final Object object) throws HBqlException {
        try {
            return this.evaluate(i, false, allowsCollections, object);
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException();
        }
    }

    protected void setContext() {
        if (this.isInNeedOfSettingContext()) {
            try {
                for (final GenericValue val : this.getExpressionList())
                    val.setExprContext(this);
            }
            catch (HBqlException e) {
                //  TODO This needs addressing
                e.printStackTrace();
            }
            this.setInNeedOfSettingContext(false);
        }
    }

    public void reset() {

        this.setInNeedOfTypeValidation(true);
        this.setInNeedOfOptimization(true);

        for (final GenericValue val : this.getExpressionList())
            val.reset();
    }

    protected void setGenericValue(final int i, final GenericValue treeRoot) {
        this.getExpressionList().set(i, treeRoot);
    }

    public void optimize() throws HBqlException {
        if (this.isInNeedOfOptimization()) {
            for (int i = 0; i < this.getExpressionList().size(); i++)
                this.setGenericValue(i, this.getGenericValue(i).getOptimizedValue());
            this.setInNeedOfOptimization(false);
        }
    }

    public void validateTypes(final boolean allowColumns, final boolean allowsCollections) throws TypeException {

        if (this.isInNeedOfTypeValidation()) {

            if (!allowColumns && this.getColumnsUsedInExpr().size() > 0)
                throw new TypeException("Invalid column reference" + (this.getColumnsUsedInExpr().size() > 1 ? "s" : "")
                                        + " in " + this.asString());

            // Collect return types of all args
            final List<Class<? extends GenericValue>> clazzList = Lists.newArrayList();
            for (final GenericValue val : this.getExpressionList())
                clazzList.add(val.validateTypes(null, allowsCollections));

            // Check against signature if there is one
            if (this.getTypeSignature() != null) {

                if (this.getExpressionList().size() != this.getTypeSignature().getArgCount())
                    throw new TypeException("Incorrect number of variables in " + this.asString());

                for (int i = 0; i < this.getTypeSignature().getArgCount(); i++) {

                    final Class<? extends GenericValue> parentClazz = this.getTypeSignature().getArg(i);
                    final Class<? extends GenericValue> clazz = clazzList.get(i);
                    // See if they are both NumberValues.  If they are, then check ranks
                    if (HUtil.isParentClass(NumberValue.class, parentClazz, clazz)) {
                        final int parentRank = NumericType.getTypeRanking(parentClazz);
                        final int clazzRank = NumericType.getTypeRanking(clazz);
                        if (clazzRank > parentRank)
                            throw new TypeException("Cannot assign a " + clazz.getSimpleName()
                                                    + " value to a " + parentClazz.getSimpleName()
                                                    + " value in " + this.asString());
                    }
                    else {
                        if (!parentClazz.isAssignableFrom(clazz))
                            throw new TypeException("Expecting type " + parentClazz.getSimpleName()
                                                    + " but found type " + clazz.getSimpleName()
                                                    + " in " + this.asString());
                    }
                }
            }

            this.setInNeedOfTypeValidation(false);
        }
    }

    public void addNamedParameter(final NamedParameter param) {

        final String name = param.getParamName();
        final List<NamedParameter> paramList;

        if (!this.getNamedParamMap().containsKey(name)) {
            paramList = Lists.newArrayList();
            this.getNamedParamMap().put(name, paramList);
        }
        else {
            paramList = this.getNamedParamMap().get(name);
        }

        paramList.add(param);
    }

    public void addColumnToUsedList(final GenericColumn column) {
        this.getColumnsUsedInExpr().add(column);
        this.getAttribsUsedInExpr().add(column.getColumnAttrib());
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        final String fullname = name.startsWith(":") ? name : (":" + name);

        if (!this.getNamedParamMap().containsKey(fullname))
            return 0;

        // Set all occurences to param value
        final List<NamedParameter> paramList = this.getNamedParamMap().get(fullname);
        for (final NamedParameter param : paramList)
            param.setParameter(val);

        this.setInNeedOfTypeValidation(true);

        return paramList.size();
    }

    private boolean isInNeedOfTypeValidation() {
        return inNeedOfTypeValidation;
    }

    private void setInNeedOfTypeValidation(final boolean inNeedOfTypeValidation) {
        this.inNeedOfTypeValidation = inNeedOfTypeValidation;
    }

    private boolean isInNeedOfOptimization() {
        return inNeedOfOptimization;
    }

    private void setInNeedOfOptimization(final boolean inNeedOfOptimization) {
        this.inNeedOfOptimization = inNeedOfOptimization;
    }

    private boolean isInNeedOfSettingContext() {
        return inNeedOfSettingContext;
    }

    private void setInNeedOfSettingContext(final boolean inNeedOfSettingContext) {
        this.inNeedOfSettingContext = inNeedOfSettingContext;
    }
}
