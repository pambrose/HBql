package org.apache.hadoop.hbase.hbql.query.impl.hbase;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 11, 2009
 * Time: 3:17:34 PM
 */
public class ObjectValue extends CurrentAndVersionValue<Object> {

    public ObjectValue(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }
}
