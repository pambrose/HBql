package org.apache.hadoop.hbase.hbql.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 11, 2009
 * Time: 1:51:09 PM
 */
public class HOutput {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(baos);

    public String toString() {
        return baos.toString();
    }
}
