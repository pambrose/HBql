package org.apache.hadoop.hbase.hbql.query.schema;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.io.Serialization;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HUtil {

    public final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static DefinedSchema getDefinedSchemaForServerFilter(final HBaseSchema schema) throws HPersistException {
        if (schema instanceof DefinedSchema)
            return (DefinedSchema)schema;
        else
            return DefinedSchema.newDefinedSchema(schema);
    }

    public static String getZeroPaddedNumber(final int val, final int width) throws HPersistException {

        final String strval = "" + val;
        final int padsize = width - strval.length();
        if (padsize < 0)
            throw new HPersistException("Value " + val + " exceeded width " + width);

        StringBuilder sbuf = new StringBuilder();
        for (int i = 0; i < padsize; i++)
            sbuf.append("0");

        sbuf.append(strval);
        return sbuf.toString();
    }

    // This keeps antlr code out of DefinedSchema, which is accessed server-side in HBase
    public static DefinedSchema newDefinedSchema(final TokenStream input,
                                                 final List<VarDesc> varList) throws RecognitionException {
        try {
            return new DefinedSchema(varList);
        }
        catch (HPersistException e) {
            System.out.println(e.getMessage());
            throw new RecognitionException(input);
        }
    }

    public static String parseStringExpr(final String s) throws HPersistException {
        final StringValue value = HBql.parseStringValue(s);
        return value.getCurrentValue(null);
    }

    public static Long parseDateExpr(final String s) throws HPersistException {
        final DateValue value = HBql.parseDateValue(s);
        return value.getCurrentValue(null);
    }

    public static Number parseNumericExpr(final String s) throws HPersistException {
        final NumberValue value = HBql.parseNumberValue(s);
        return value.getCurrentValue(null);
    }

    public static void logException(final Log log, final Exception e) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintWriter oos = new PrintWriter(baos);

        e.printStackTrace(oos);
        oos.flush();
        oos.close();

        log.info(baos.toString());

    }
}
