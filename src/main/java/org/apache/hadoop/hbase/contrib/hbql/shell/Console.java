package org.apache.hadoop.hbase.contrib.hbql.shell;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import org.apache.expreval.util.Lists;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class Console {

    public static void usage() {
        System.out.println("Usage: java " + Console.class.getName()
                           + " [none/simple/files/dictionary [trigger mask]]");
        System.out.println("  none - no completors");
        System.out.println("  simple - a simple completor that comples "
                           + "\"foo\", \"bar\", and \"baz\"");
        System.out
                .println("  files - a completor that comples " + "file names");
        System.out.println("  dictionary - a completor that comples "
                           + "english dictionary words");
        System.out.println("  classes - a completor that comples "
                           + "java class names");
        System.out
                .println("  trigger - a special word which causes it to assume "
                         + "the next line is a password");
        System.out.println("  mask - is the character to print in place of "
                           + "the actual password character");
        System.out.println("\n  E.g - java Example simple su '*'\n"
                           + "will use the simple compleator with 'su' triggering\n"
                           + "the use of '*' as a password mask.");
    }

    public static void main(String[] args) throws IOException {

        final ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));

        List<SimpleCompletor> completors = Lists.newArrayList();

        completors.add(new SimpleCompletor(new String[]{"select", "insert",
                                                        "baz"}));

        reader.addCompletor(new ArgumentCompletor(completors));

        String line;
        PrintWriter out = new PrintWriter(System.out);

        while ((line = reader.readLine("prompt> ")) != null) {
            out.println("======>\"" + line + "\"");
            out.flush();

            if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                break;
            }
        }
    }
}
