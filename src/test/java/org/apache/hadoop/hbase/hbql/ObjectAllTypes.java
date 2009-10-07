package org.apache.hadoop.hbase.hbql;

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