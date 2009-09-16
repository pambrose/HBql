package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.antlr.args.DateRangeArgs;
import com.imap4j.hbase.antlr.args.KeyRangeArgs;
import com.imap4j.hbase.antlr.args.LimitArgs;
import com.imap4j.hbase.antlr.args.VersionArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Lists;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    public static List<Scan> getScanList(final HBaseSchema schema,
                                         final List<String> fieldList,
                                         final KeyRangeArgs keyRangeArgs,
                                         final DateRangeArgs dateRangeArgs,
                                         final VersionArgs versionArgs,
                                         final LimitArgs limitArgs,
                                         final ExprTree serverFilter) throws IOException, HPersistException {

        final List<Scan> scanList = Lists.newArrayList();
        final List<KeyRangeArgs.Range> rangeList = keyRangeArgs.getRangeList();

        if (rangeList.size() == 0) {
            scanList.add(new Scan());
        }
        else {
            for (final KeyRangeArgs.Range range : rangeList) {
                final Scan scan = new Scan();
                scan.setStartRow(range.getLowerAsBytes());
                if (!range.isStartKeyOnly())
                    scan.setStopRow(range.getUpperAsBytes());
                scanList.add(scan);
            }
        }

        for (final Scan scan : scanList) {

            // Set column names
            for (final String name : fieldList) {
                final ColumnAttrib attrib;
                try {
                    attrib = (ColumnAttrib)schema.getVariableAttribByVariableName(name);
                }
                catch (HPersistException e) {
                    throw new HPersistException("Element " + name + " does not exist in " + schema.getSchemaName());
                }

                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyName().getBytes());
                else
                    scan.addColumn(attrib.getFamilyQualifiedName().getBytes());
            }

            if (dateRangeArgs != null && dateRangeArgs.isValid()) {
                if (dateRangeArgs.getLower() == dateRangeArgs.getUpper())
                    scan.setTimeStamp(dateRangeArgs.getLower());
                else
                    scan.setTimeRange(dateRangeArgs.getLower(), dateRangeArgs.getUpper());
            }

            if (versionArgs != null && versionArgs.isValid()) {
                final int max = versionArgs.getValue();
                // -999 indicates MAX versions requested
                if (max == -999)
                    scan.setMaxVersions();
                else
                    scan.setMaxVersions(max);
            }

            // Set server-side filter.  It must be a DefinedSchema since the server uses HRecord evaluation
            if (serverFilter != null) {

                // final List<ExprVariable> names = serverFilter.getExprVariables();
                // boolean okay = HUtil.ser.isSerializable(serverSchema) && HUtil.ser.isSerializable(serverFilter);

                final long limit = (limitArgs != null) ? limitArgs.getValue() : 0;
                final HBqlFilter filter = new HBqlFilter(serverFilter, limit);
                //HBqlFilter.testFilter(filter);
                scan.setFilter(filter);
            }
        }

        return scanList;
    }

    public static DefinedSchema getServerSchema(final HBaseSchema schema) throws HPersistException {
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
        final StringValue value = (StringValue)HBqlRule.STRING_EXPR.parse(s);
        return value.getCurrentValue(null);
    }

    public static Long parseDateExpr(final String s) throws HPersistException {
        final DateValue value = (DateValue)HBqlRule.DATE_EXPR.parse(s);
        return value.getCurrentValue(null);
    }

    public static Number parseNumericExpr(final String s) throws HPersistException {
        final NumberValue value = (NumberValue)HBqlRule.NUMBER_EXPR.parse(s);
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

    public static ExprTree parseExprTree(final HBqlRule rule,
                                         final String query,
                                         final ExprSchema schema,
                                         final boolean optimize) throws HPersistException {
        final ExprTree exprTree = (ExprTree)rule.parse(query, schema);
        exprTree.setSchema(schema);
        if (optimize)
            exprTree.optimize();
        return exprTree;
    }
}
