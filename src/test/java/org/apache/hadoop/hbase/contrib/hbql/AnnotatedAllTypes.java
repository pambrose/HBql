package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.hadoop.hbase.contrib.hbql.client.Column;
import org.apache.hadoop.hbase.contrib.hbql.client.Family;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Table;
import org.apache.hadoop.hbase.contrib.hbql.client.Util;

import java.io.Serializable;
import java.util.Date;

@Table(name = "alltypes",
       families = {
               @Family(name = "family1", maxVersions = 10)
       })
public class AnnotatedAllTypes implements Serializable {

    @Column(key = true)
    public String keyval = null;

    @Column(family = "family1")
    public boolean booleanValue = true;

    @Column(family = "family1")
    public boolean[] booleanArrayValue = null;

    @Column(family = "family1")
    public byte byteValue = 0;

    @Column(family = "family1")
    public byte[] byteArrayValue = null;

    @Column(family = "family1")
    public short shortValue = 0;

    @Column(family = "family1")
    public short[] shortArrayValue;

    @Column(family = "family1")
    public int intValue = 0;

    @Column(family = "family1")
    public int[] intArrayValue;

    @Column(family = "family1")
    public long longValue = 0L;

    @Column(family = "family1")
    public long[] longArrayValue = null;

    @Column(family = "family1")
    public float floatValue = 0;

    @Column(family = "family1")
    public float[] floatArrayValue = null;

    @Column(family = "family1")
    public double doubleValue = 0;

    @Column(family = "family1")
    public double[] doubleArrayValue = null;

    @Column(family = "family1")
    public String stringValue = "";

    @Column(family = "family1")
    public String[] stringArrayValue = null;

    @Column(family = "family1")
    public Date dateValue = new Date();

    @Column(family = "family1")
    public Date[] dateArrayValue = null;

    @Column(family = "family1")
    public TestObject objectValue = new TestObject();

    @Column(family = "family1")
    public TestObject[] objectArrayValue = null;


    public static class TestObject implements Serializable {
        public int intvalue = -9;
    }

    public AnnotatedAllTypes() {
    }

    public AnnotatedAllTypes(final String keyval, final int intValue, final String stringValue) {
        this.keyval = keyval;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    public void setATestValue(int val) throws HBqlException {

        this.keyval = Util.getZeroPaddedNumber(val, 10);
        this.booleanValue = val % 2 == 0;
    }
}
