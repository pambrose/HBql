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
import com.imap4j.hbase.util.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HUtil {

    public final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static List<Scan> getScanList(final ExprSchema schema,
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
                    throw new HPersistException("Instance variable " + name
                                                + " does not exist in " + schema.getSchemaName());

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

    public static Object getObject(final Serialization ser,
                                   final ExprSchema schema,
                                   final List<String> fieldList,
                                   final int maxVersions,
                                   final Result result) throws HPersistException {

        try {
            // Create object and assign key value
            final Object newobj = createNewObject(ser, (AnnotationSchema)schema, result);

            // Assign most recent values
            assignCurrentValues(ser, (AnnotationSchema)schema, fieldList, result, newobj);

            // Assign the versioned values
            if (maxVersions > 1)
                assignVersionedValues((AnnotationSchema)schema, fieldList, result, newobj);

            return newobj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HPersistException("Error in getObject()");
        }
    }

    private static Object createNewObject(final Serialization ser,
                                          final AnnotationSchema schema,
                                          final Result result) throws IOException, HPersistException {

        // Create new instance and set key value
        final ColumnAttrib keyattrib = schema.getKeyColumnAttrib();
        final Object newobj;
        try {
            newobj = schema.newInstance();
            final byte[] keybytes = result.getRow();
            keyattrib.setValue(ser, newobj, keybytes);
        }
        catch (InstantiationException e) {
            throw new RuntimeException("Cannot create new instance of " + schema.getSchemaName());
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot set value for key  " + keyattrib.getVariableName()
                                       + " for " + schema.getSchemaName());
        }
        return newobj;
    }

    private static void assignCurrentValues(final Serialization ser,
                                            final AnnotationSchema schema,
                                            final List<String> fieldList,
                                            final Result result,
                                            final Object newobj) throws IOException, HPersistException {

        for (final KeyValue keyValue : result.list()) {

            final byte[] cbytes = keyValue.getColumn();
            final byte[] vbytes = result.getValue(cbytes);
            final String colname = ser.getStringFromBytes(cbytes);

            if (colname.endsWith("]")) {
                final int lbrace = colname.indexOf("[");
                final String mapcolumn = colname.substring(0, lbrace);
                final String mapKey = colname.substring(lbrace + 1, colname.length() - 1);
                final ColumnAttrib attrib = schema.getColumnAttribByFamilyQualifiedColumnName(mapcolumn);
                final Object val = attrib.getValueFromBytes(ser, newobj, vbytes);

                Map mapval = (Map)attrib.getValue(newobj);

                // TODO Need to check if variable was on select list like below

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    attrib.setValue(newobj, mapval);
                }

                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = schema.getColumnAttribByFamilyQualifiedColumnName(colname);

                // Check if variable was requested in select list
                if (fieldList.contains(attrib.getField().getName()))
                    attrib.setValue(ser, newobj, vbytes);
            }
        }
    }

    private static void assignVersionedValues(final AnnotationSchema schema,
                                              final List<String> fieldList,
                                              final Result result,
                                              final Object newobj) throws IOException, HPersistException {

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

        for (final byte[] fbytes : familyMap.keySet()) {

            final String famname = ser.getStringFromBytes(fbytes) + ":";
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(fbytes);

            for (final byte[] cbytes : columnMap.keySet()) {
                final String colname = ser.getStringFromBytes(cbytes);
                final String qualifiedName = famname + colname;
                final NavigableMap<Long, byte[]> tsMap = columnMap.get(cbytes);

                for (final Long timestamp : tsMap.keySet()) {
                    final byte[] vbytes = tsMap.get(timestamp);

                    final VersionAttrib attrib = schema.getVersionAttribByFamilyQualifiedColumnName(qualifiedName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!fieldList.contains(attrib.getField().getName()))
                        continue;

                    final Object val = attrib.getValueFromBytes(ser, newobj, vbytes);
                    Map mapval = (Map)attrib.getValue(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setValue(newobj, mapval);
                    }

                    mapval.put(timestamp, val);
                }
            }
        }
    }

}
