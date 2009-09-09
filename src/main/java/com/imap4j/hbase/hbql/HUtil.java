package com.imap4j.hbase.hbql;

import com.google.common.collect.Lists;
import com.imap4j.hbase.antlr.args.KeyRangeArgs;
import com.imap4j.hbase.antlr.args.VersionArgs;
import com.imap4j.hbase.antlr.args.WhereArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.hbql.schema.ColumnAttrib;
import com.imap4j.hbase.hbql.schema.ExprSchema;
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

    public static List<Scan> getScanList(final ExprSchema exprSchema, final List<String> fieldList, final WhereArgs whereExpr) throws IOException, HPersistException {

        final List<Scan> scanList = Lists.newArrayList();
        final KeyRangeArgs keys = whereExpr.getKeyRangeArgs();
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
            for (final String attribName : fieldList) {
                final ColumnAttrib attrib = (ColumnAttrib)exprSchema.getVariableAttribByVariableName(attribName);

                if (attrib == null)
                    throw new HPersistException("Instance variable " + exprSchema.getClazz().getName()
                                                + "." + attribName + " does not exist");

                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyName().getBytes());
                else
                    scan.addColumn(attrib.getFamilyQualifiedName().getBytes());
            }

            final VersionArgs verArgs = whereExpr.getVersionArgs();
            if (verArgs.isValid())
                scan.setMaxVersions(verArgs.getValue());

            // Set server-side filter
            final ExprEvalTree serverFilter = whereExpr.getServerFilterArgs();
            if (serverFilter != null) {
                List<ExprVariable> names = serverFilter.getExprVariables();
                scan.setFilter(new HBqlFilter(exprSchema, serverFilter));
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
