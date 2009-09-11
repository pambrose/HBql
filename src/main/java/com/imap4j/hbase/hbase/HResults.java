package com.imap4j.hbase.hbase;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 11, 2009
 * Time: 1:51:09 PM
 */
public class HResults {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(baos);

    public String getOutput() {
        return baos.toString();
    }
}
