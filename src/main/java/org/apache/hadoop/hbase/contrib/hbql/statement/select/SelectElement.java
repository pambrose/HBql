package org.apache.hadoop.hbase.contrib.hbql.statement.select;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.contrib.hbql.statement.SelectStatement;

import java.util.List;

public interface SelectElement {

    void validate(HBaseSchema schema, Connection connection) throws HBqlException;

    List<ColumnAttrib> getAttribsUsedInExpr();

    void assignAsNamesForExpressions(SelectStatement selectStatement);

    void assignValues(Object newobj,
                      int maxVerions,
                      Result result) throws HBqlException;

    int setParameter(String name, Object val) throws HBqlException;

    String getAsName();

    String getElementName();

    boolean hasAsName();

    boolean isAFamilySelect();

    String asString();
}
