package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.NamedParameter;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 28, 2009
 * Time: 9:56:12 PM
 */
public abstract class ExprContext {

    private boolean inNeedOfTypeValidation = true;
    private boolean inNeedOfOptimization = true;
    private boolean inNeedOfSettingContext = true;

    private final List<GenericColumn> columnList = Lists.newArrayList();
    private final Map<String, List<NamedParameter>> namedParamMap = Maps.newHashMap();

    private Schema schema = null;
    private final TypeSignature typeSignature;
    private List<GenericValue> genericValues = Lists.newArrayList();

    protected ExprContext(final TypeSignature typeSignature, final GenericValue... vals) {
        this.typeSignature = typeSignature;
        if (vals != null)
            for (final GenericValue val : vals)
                this.addGenericValue(val);
    }

    public abstract String asString();

    public abstract boolean useHBaseResult();

    public List<GenericColumn> getColumnList() {
        return this.columnList;
    }

    public void addGenericValue(final GenericValue genericValue) {
        this.getGenericValues().add(genericValue);
    }

    public List<ColumnAttrib> getFamilyQualifiedColumnAttribList() {
        final List<ColumnAttrib> retval = Lists.newArrayList();
        for (final GenericColumn col : this.getColumnList())
            retval.add(col.getColumnAttrib());
        return retval;
    }

    public Map<String, List<NamedParameter>> getNamedParamMap() {
        return this.namedParamMap;
    }

    protected List<GenericValue> getGenericValues() {
        return this.genericValues;
    }

    private TypeSignature getTypeSignature() {
        return this.typeSignature;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
        if (this.isValid())
            this.setContext();
    }

    protected GenericValue getGenericValue(final int i) {
        return this.getGenericValues().get(i);
    }

    public boolean isValid() {
        if (this.getGenericValues().size() == 0)
            return false;

        for (final GenericValue val : this.getGenericValues())
            if (val == null)
                return false;
        return true;
    }

    protected void setContext() {
        if (this.isValid() && this.isInNeedOfSettingContext()) {
            try {
                for (final GenericValue val : this.getGenericValues())
                    val.setExprContext(this);
            }
            catch (HBqlException e) {
                e.printStackTrace();
            }
            this.setInNeedOfSettingContext(false);
        }
    }

    protected void setGenericValue(final int i, final GenericValue treeRoot) {
        this.getGenericValues().set(i, treeRoot);
    }

    public void optimize() throws HBqlException {
        if (this.isValid() && this.isInNeedOfOptimization()) {
            for (int i = 0; i < this.getGenericValues().size(); i++)
                this.setGenericValue(i, this.getGenericValue(i).getOptimizedValue());
            this.setInNeedOfOptimization(false);
        }
    }

    public void validateTypes(final boolean allowColumns) throws TypeException {

        if (this.isValid() && this.isInNeedOfTypeValidation()) {

            if (!allowColumns && this.getColumnList().size() > 0) {
                throw new TypeException("Invalid column reference" + (this.getColumnList().size() > 1 ? "s" : "")
                                        + " in " + this.asString());
            }

            // Collect return types of all args
            final List<Class<? extends GenericValue>> clazzList = Lists.newArrayList();
            for (final GenericValue val : this.getGenericValues())
                clazzList.add(val.validateTypes(null, false));

            // Check against signature if there is one
            if (this.getTypeSignature() != null) {

                if (this.getGenericValues().size() != this.getTypeSignature().getArgCount())
                    throw new TypeException("Incorrect number of variables in " + this.asString());

                for (int i = 0; i < this.getTypeSignature().getArgCount(); i++) {

                    final Class<? extends GenericValue> parentClazz = this.getTypeSignature().getArg(i);

                    if (!parentClazz.isAssignableFrom(clazzList.get(i)))
                        throw new TypeException("Expecting type " + parentClazz.getSimpleName()
                                                + " but encountered type " + clazzList.get(i).getSimpleName()
                                                + " in " + this.asString());
                }
            }
        }
        this.setInNeedOfTypeValidation(false);
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

    public void addVariable(final GenericColumn column) {
        this.getColumnList().add(column);
    }

    public void setParameter(final String str, final Object val) throws HBqlException {

        final String name = str.startsWith(":") ? str : (":" + str);

        if (!this.getNamedParamMap().containsKey(name))
            throw new HBqlException("Parameter name " + str + " does not exist in " + this.asString());

        final List<NamedParameter> paramList = this.getNamedParamMap().get(name);
        for (final NamedParameter param : paramList)
            param.setParameter(val);

        this.setInNeedOfTypeValidation(true);
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
