package org.apache.hadoop.hbase.hbql;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 7:41:00 AM
 */
public class ObjectAllTypes {

    private String keyval = null;
    private int intValue = -1;
    private String stringValue = "";

    public ObjectAllTypes() {
    }

    public ObjectAllTypes(final String keyval, final int intValue, final String stringValue) {
        this.keyval = keyval;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }
}