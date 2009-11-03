package org.apache.hadoop.hbase.contrib.hbql.io;

public class IO {
    public final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static Serialization getSerialization() {
        return ser;
    }
}
