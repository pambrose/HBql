package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.stmt.schema.InsertStatement;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;

import java.io.IOException;
import java.util.Iterator;

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

    public void validate(final InsertStatement insertStatement) throws HBqlException {
        super.validate(insertStatement);

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
        HQuery<HRecord> query = this.getInsertStatement().getConnection().newHQuery(this.getSelectStatement());
        HResults<HRecord> results = query.getResults();
        this.setResultsIterator(results.iterator());
    }

    public void reset() {

    }

    public String asString() {
        return this.getSelectStatement().asString();
    }

    public int getValueCount() {
        return this.getSelectStatement().getSelectElementList().size();
    }

    public Object getValue(final int i) throws HBqlException {
        return this.getCurrentRecord().get(i);
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
}