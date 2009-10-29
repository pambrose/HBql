package org.apache.hadoop.hbase.contrib.hbql;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ParseException;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.contrib.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.parser.HBqlParser;
import org.apache.hadoop.hbase.contrib.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.SchemaManagerStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.ShellStatement;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class Console {

    public static void main(String[] args) throws IOException {

        final List<SimpleCompletor> completors = Lists.newArrayList();
        completors.add(new SimpleCompletor(new String[]{"select", "insert", "create", "table", "schema", "describe"}));

        final ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        reader.setUseHistory(true);
        //reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
        reader.addCompletor(new ArgumentCompletor(completors));

        final PrintWriter out = new PrintWriter(System.out);

        final ConnectionImpl conn = (ConnectionImpl)HConnectionManager.newHConnection();

        while (true) {

            final String line = reader.readLine("HBql> ");

            if (line == null || line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit"))
                break;

            try {
                final List<ShellStatement> stmtList = HBqlParser.parseCommands(line);
                for (final ShellStatement stmt : stmtList) {
                    if (stmt instanceof ConnectionStatement)
                        out.println(((ConnectionStatement)stmt).execute(conn));
                    else if (stmt instanceof SchemaManagerStatement)
                        out.println(((SchemaManagerStatement)stmt).execute());
                    else
                        throw new InternalErrorException("Unknown statement type");
                }
            }
            catch (ParseException e) {
                out.println("Error in input: ");
                out.println(e.getMessage());
                if (e.getRecognitionException() != null) {
                    final StringBuilder sbuf = new StringBuilder();
                    for (int i = 0; i < e.getRecognitionException().charPositionInLine; i++)
                        sbuf.append("-");
                    sbuf.append("^");
                    out.println(sbuf.toString());
                }
            }
            catch (HBqlException e) {
                out.println("Error in input: " + line);
                out.println(e.getMessage());
            }

            out.flush();
        }
    }
}
