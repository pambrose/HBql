package org.apache.hadoop.hbase.hbql.stmt.select;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.stmt.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.stmt.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;

import java.util.List;

public interface SelectElement {

    void validate(HBaseSchema schema, HConnection connection) throws HBqlException;

    List<ColumnAttrib> getAttribsUsedInExpr();

    void assignAsNamesForExpressions(SelectStatement selectStatement);

    void assignValues(Object newobj,
                      List<ColumnAttrib> selectAttribList,
                      int maxVerions,
                      Result result) throws HBqlException;

    int setParameter(String name, Object val) throws HBqlException;

    String getAsName();

    boolean hasAsName();

    String asString();
}
