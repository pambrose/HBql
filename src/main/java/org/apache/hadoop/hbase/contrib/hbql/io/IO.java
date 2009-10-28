package org.apache.hadoop.hbase.contrib.hbql.io;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 28, 2009
 * Time: 3:36:03 PM
 */
public class IO {
    public final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static Serialization getSerialization() {
        return ser;
    }
}
