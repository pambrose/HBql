package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericAttribRef;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.NamedParameter;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree implements Serializable {

    private final Map<String, List<NamedParameter>> namedParamsMap = Maps.newHashMap();
    private final List<ExprVariable> exprVariablesList = Lists.newArrayList();

    private boolean inNeedOfTypeValidation = true;
    private boolean inNeedOfOptimization = true;
    private boolean inNeedOfSettingContext = true;

    private Schema schema = null;
    private ValueExpr treeRoot = null;
    private long start, end;

    private ExprTree() {
    }

    public static ExprTree newExprTree(final BooleanValue booleanValue) {
        final ExprTree tree = new ExprTree();
        tree.setTreeRoot(booleanValue);
        return tree;
    }

    private ValueExpr getTreeRoot() {
        return this.treeRoot;
    }

    private void setTreeRoot(final ValueExpr treeRoot) {
        this.treeRoot = treeRoot;

        if (this.getTreeRoot() != null && this.isInNeedOfSettingContext()) {
            this.getTreeRoot().setContext(this);
            this.setInNeedOfSettingContext(false);
        }
    }

    public Schema getSchema() {
        return this.schema;
    }

    public boolean isValid() {
        return this.getTreeRoot() != null;
    }

    public void setSchema(final Schema schema) {
        if (schema != null)
            this.schema = schema;
    }

    public void addNamedParameter(final NamedParameter param) {
        final String name = param.getParamName();
        final List<NamedParameter> paramList;
        if (!this.namedParamsMap.containsKey(name)) {
            paramList = Lists.newArrayList();
            this.namedParamsMap.put(name, paramList);
        }
        else {
            paramList = this.namedParamsMap.get(name);
        }
        paramList.add(param);
    }

    public void addAttribRef(final GenericAttribRef attribRef) {
        this.getExprVariablesList().add(attribRef.getExprVar());
    }


    public void setParameter(final String str, final Object val) throws HBqlException {

        final String name = str.startsWith(":") ? str : (":" + str);

        if (!this.namedParamsMap.containsKey(name))
            throw new HBqlException("Parameter name " + str + " does not exist");

        final List<NamedParameter> paramList = this.namedParamsMap.get(name);
        for (final NamedParameter param : paramList)
            param.setParameter(val);

        this.setInNeedOfTypeValidation(true);
    }

    private void optimize() throws HBqlException {
        this.setTreeRoot(this.getTreeRoot().getOptimizedValue());
        this.setInNeedOfOptimization(false);
    }

    private void validateTypes() throws HBqlException {
        this.getTreeRoot().validateTypes();
        this.setInNeedOfTypeValidation(false);
    }

    public List<ExprVariable> getExprVariablesList() {
        return this.exprVariablesList;
    }

    public Boolean evaluate(final Object object) throws HBqlException {

        if (this.isInNeedOfTypeValidation())
            this.validateTypes();

        if (this.isInNeedOfOptimization())
            this.optimize();

        // Set it once per evaluation
        DateLiteral.resetNow();

        this.start = System.nanoTime();
        final boolean retval = (this.getTreeRoot() == null) || (Boolean)this.getTreeRoot().getValue(object);
        this.end = System.nanoTime();

        return retval;
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }

    public void setSchema(final Schema schema, final List<String> fieldList) throws HBqlException {

        if (this.isValid()) {
            this.setSchema(schema);

            // Check if all the variables referenced in the where clause are present in the fieldList.
            final List<String> selectList = schema.getAliasAndQualifiedNameFieldList(fieldList);

            final List<ExprVariable> referencedVars = this.getExprVariablesList();
            for (final ExprVariable var : referencedVars) {
                if (!selectList.contains(var.getName()))
                    throw new HBqlException("Variable " + var.getName() + " used in where clause but it is not "
                                            + "not in the select list");
            }
        }
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

    public boolean isInNeedOfSettingContext() {
        return inNeedOfSettingContext;
    }

    public void setInNeedOfSettingContext(final boolean inNeedOfSettingContext) {
        this.inNeedOfSettingContext = inNeedOfSettingContext;
    }
}