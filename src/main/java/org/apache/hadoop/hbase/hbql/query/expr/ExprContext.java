package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.NamedParameter;
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
public class ExprContext {

    private boolean inNeedOfTypeValidation = true;
    private boolean inNeedOfOptimization = true;
    private boolean inNeedOfSettingContext = true;

    private final List<GenericColumn> columnList = Lists.newArrayList();
    private final Map<String, List<NamedParameter>> namedParamMap = Maps.newHashMap();

    private Schema schema = null;
    private GenericValue genericValue = null;

    public List<GenericColumn> getColumnList() {
        return this.columnList;
    }

    public List<String> getFamilyQualifiedColumnNameList() {
        final List<String> nameList = Lists.newArrayList();

        for (final GenericColumn col : this.getColumnList())
            nameList.add(col.getFamilyQualifiedName());

        return nameList;
    }

    public Map<String, List<NamedParameter>> getNamedParamMap() {
        return this.namedParamMap;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
        this.setContext();
    }

    protected GenericValue getGenericValue() {
        return this.genericValue;
    }

    public boolean isValid() {
        return this.getGenericValue() != null;
    }

    private void setContext() {
        if (this.getGenericValue() != null && this.isInNeedOfSettingContext()) {
            try {
                this.getGenericValue().setContext(this);
            }
            catch (HBqlException e) {
                e.printStackTrace();
            }
            this.setInNeedOfSettingContext(false);
        }
    }

    protected void setGenericValue(final GenericValue treeRoot) {
        this.genericValue = treeRoot;

    }

    protected void optimize() throws HBqlException {
        if (this.isInNeedOfOptimization()) {
            this.setGenericValue(this.getGenericValue().getOptimizedValue());
            this.setInNeedOfOptimization(false);
        }
    }

    protected void validateTypes() throws HBqlException {
        if (this.isInNeedOfTypeValidation()) {
            this.getGenericValue().validateTypes(null, false);
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

    public void addVariable(final GenericColumn column) {
        this.getColumnList().add(column);
    }

    public void setParameter(final String str, final Object val) throws HBqlException {

        final String name = str.startsWith(":") ? str : (":" + str);

        if (!this.getNamedParamMap().containsKey(name))
            throw new HBqlException("Parameter name " + str + " does not exist");

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
