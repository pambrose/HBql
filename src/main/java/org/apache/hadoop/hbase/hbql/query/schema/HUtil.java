package org.apache.hadoop.hbase.hbql.query.schema;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.io.Serialization;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

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

    public static DefinedSchema getDefinedSchemaForServerFilter(final HBaseSchema schema) throws HBqlException {
        if (schema instanceof DefinedSchema)
            return (DefinedSchema)schema;
        else
            return DefinedSchema.newDefinedSchema(schema);
    }

    public static String getZeroPaddedNumber(final int val, final int width) throws HBqlException {

        final String strval = "" + val;
        final int padsize = width - strval.length();
        if (padsize < 0)
            throw new HBqlException("Value " + val + " exceeded width " + width);

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
        catch (HBqlException e) {
            System.out.println(e.getMessage());
            throw new RecognitionException(input);
        }
    }

    public static void logException(final Log log, final Exception e) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintWriter oos = new PrintWriter(baos);

        e.printStackTrace(oos);
        oos.flush();
        oos.close();

        log.info(baos.toString());
    }

    public static boolean isParentClass(final Class parentClazz, final Class... clazzes) {

        for (final Class clazz : clazzes) {

            if (clazz == null)
                continue;

            if (!parentClazz.isAssignableFrom(clazz))
                return false;
        }
        return true;
    }

    public static void validateParentClass(final ValueExpr expr,
                                           final Class parentClazz,
                                           final Class... clazzes) throws TypeException {

        List<Class> classList = null;

        for (final Class clazz : clazzes) {

            if (clazz == null)
                continue;

            if (!parentClazz.isAssignableFrom(clazz)) {
                if (classList == null)
                    classList = Lists.newArrayList();
                classList.add(clazz);
            }
        }

        if (classList != null) {
            final StringBuilder sbuf = new StringBuilder("Expecting type " + parentClazz.getName()
                                                         + " but encountered type "
                                                         + ((classList.size() > 0) ? "s" : "") + ": ");
            boolean first = true;
            for (final Class clazz : classList) {
                if (!first)
                    sbuf.append(", ");
                sbuf.append(clazz.getName());
                first = false;
            }

            sbuf.append(" in expression: " + expr.asString());

            throw new TypeException(sbuf.toString());
        }
    }

    public static void reportTypeConflict(final ValueExpr expr,
                                          final Class<? extends ValueExpr>... clazzes) throws TypeException {

        final StringBuilder sbuf = new StringBuilder("Incompatible types ");
        boolean first = true;
        for (final Class<? extends ValueExpr> clazz : clazzes) {
            if (clazz == null)
                continue;
            if (!first)
                sbuf.append(", ");
            sbuf.append(clazz.getSimpleName());
            first = false;
        }
        sbuf.append(" in expression: " + expr.asString());

        throw new TypeException(sbuf.toString());
    }
}
