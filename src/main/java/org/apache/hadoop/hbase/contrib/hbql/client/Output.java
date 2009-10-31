package org.apache.hadoop.hbase.contrib.hbql.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class Output {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    public final PrintStream out = new PrintStream(baos);

    public Output() {
    }

    public Output(final String str) {
        this.out.println(str);
    }

    public String toString() {
        this.out.flush();
        return baos.toString();
    }
}
