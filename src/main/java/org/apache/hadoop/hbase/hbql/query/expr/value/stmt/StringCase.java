package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

import java.util.List;

public class StringCase extends GenericCase implements StringValue {

    public StringCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(Type.STRINGCASE, whenExprList, elseExpr);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}