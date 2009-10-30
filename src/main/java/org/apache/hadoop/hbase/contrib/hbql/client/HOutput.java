package org.apache.hadoop.hbase.contrib.hbql.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class HOutput {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    public final PrintStream out = new PrintStream(baos);

    public HOutput() {
    }

    public HOutput(final String str) {
        this.out.println(str);
    }

    public String toString() {
        this.out.flush();
        return baos.toString();
    }
}
