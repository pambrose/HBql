package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;
import org.apache.hadoop.hbase.hbql.stmt.select.ExprElement;
import org.apache.hadoop.hbase.hbql.stmt.select.SelectElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class InsertSelectValues extends InsertValueSource {

    private final SelectStatement selectStatement;
    private Iterator<HRecord> resultsIterator = null;
    private HRecord currentRecord = null;


    public InsertSelectValues(final SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public SelectStatement getSelectStatement() {
        return this.selectStatement;
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        return this.getSelectStatement().setParameter(name, val);
    }

    public void validate() throws HBqlException {

        for (final SelectElement element : this.getSelectStatement().getSelectElementList()) {
            if (element.isAFamilySelect())
                throw new TypeException("Family select items are not valid in INSERT statement");
        }

        this.getSelectStatement().validate(this.getInsertStatement().getConnection());
    }

    private Iterator<HRecord> getResultsIterator() {
        return this.resultsIterator;
    }

    private void setResultsIterator(final Iterator<HRecord> resultsIterator) {
        this.resultsIterator = resultsIterator;
    }

    private HRecord getCurrentRecord() {
        return this.currentRecord;
    }

    private void setCurrentRecord(final HRecord currentRecord) {
        this.currentRecord = currentRecord;
    }

    public void execute() throws HBqlException, IOException {
        final HQuery<HRecord> query = this.getInsertStatement().getConnection().newHQuery(this.getSelectStatement());
        final HResults<HRecord> results = query.getResults();
        this.setResultsIterator(results.iterator());
    }

    public List<Class<? extends GenericValue>> getValuesTypeList() throws HBqlException {
        final List<Class<? extends GenericValue>> typeList = Lists.newArrayList();
        for (final SelectElement element : this.getSelectStatement().getSelectElementList()) {
            if (element instanceof ExprElement) {
                final ExprElement expr = (ExprElement)element;
                final Class<? extends GenericValue> type = expr.getExpressionType();
                typeList.add(type);
            }
        }
        return typeList;
    }

    public void reset() {

    }

    public String asString() {
        return this.getSelectStatement().asString();
    }

    public Object getValue(final int i) throws HBqlException {
        final SelectElement element = this.getSelectStatement().getSelectElementList().get(i);
        final String name = element.getElementName();
        return this.getCurrentRecord().getCurrentValue(name);
    }

    public boolean isDefaultValue(final int i) throws HBqlException {
        return false;
    }

    public boolean hasValues() {
        if (this.getResultsIterator().hasNext()) {
            this.setCurrentRecord(this.getResultsIterator().next());
            return true;
        }
        else {
            return false;
        }
    }
}