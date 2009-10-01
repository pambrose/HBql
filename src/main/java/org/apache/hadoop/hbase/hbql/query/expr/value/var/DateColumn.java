package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.util.Date;
import java.util.NavigableMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateColumn extends GenericColumn<DateValue> implements DateValue {

    public DateColumn(final VariableAttrib attrib) {
        super(attrib);
    }

    @Override
    protected FieldType getFieldType() {
        return FieldType.DateType;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        if (this.getExprContext().useHBaseResult()) {
            final Result result = (Result)object;
            final NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = result.getNoVersionMap();
            final NavigableMap<byte[], byte[]> columnMap = familyMap.get(this.getVariableAttrib().getFamilyNameBytes());
            final byte[] val = columnMap.get(this.getVariableAttrib().getColumnNameBytes());
            final Date date = (Date)HUtil.ser.getScalarFromBytes(this.getFieldType(), val);
            return date.getTime();
        }
        else {
            return ((Date)this.getVariableAttrib().getCurrentValue(object)).getTime();
        }
    }

}