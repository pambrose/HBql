package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.antlr.args.KeyRangeArgs;
import com.imap4j.hbase.antlr.args.VersionArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Lists;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;

import java.io.IOException;
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
                                         final KeyRangeArgs keys,
                                         final VersionArgs verArgs,
                                         final ExprTree serverFilter) throws IOException, HPersistException {

        final List<Scan> scanList = Lists.newArrayList();
        final List<KeyRangeArgs.Range> rangeList = keys.getRangeList();

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
                final ColumnAttrib attrib = (ColumnAttrib)schema.getVariableAttribByVariableName(name);

                if (attrib == null)
                    throw new HPersistException("Element " + name + " does not exist in " + schema.getSchemaName());

                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyName().getBytes());
                else
                    scan.addColumn(attrib.getFamilyQualifiedName().getBytes());
            }

            if (verArgs != null && verArgs.isValid())
                scan.setMaxVersions(verArgs.getValue());

            // Set server-side filter
            if (serverFilter != null) {
                serverFilter.setSchema(schema);
                serverFilter.optimize();

                final List<ExprVariable> names = serverFilter.getExprVariables();
                scan.setFilter(new HBqlFilter(schema, serverFilter));
            }
        }

        return scanList;
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

    public static String parseStringExpr(final String s) throws HPersistException {
        final StringValue value = (StringValue)HBqlRule.STRING_EXPR.parse(s);
        return value.getValue(null);
    }

    public static Long parseDateExpr(final String s) throws HPersistException {
        final DateValue value = (DateValue)HBqlRule.DATE_EXPR.parse(s);
        return value.getValue(null);
    }

    public static Number parseNumericExpr(final String s) throws HPersistException {
        final NumberValue value = (NumberValue)HBqlRule.NUMBER_EXPR.parse(s);
        return value.getValue(null);
    }

}
