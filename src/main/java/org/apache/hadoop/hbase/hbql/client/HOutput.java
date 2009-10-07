package org.apache.hadoop.hbase.hbql.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class HOutput {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    public final PrintStream out = new PrintStream(baos);

    public String toString() {
        return baos.toString();
    }
}
