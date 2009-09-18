package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.antlr.args.DateRangeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.KeyRangeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.VersionArgs;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.io.Serialization;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 13, 2009
 * Time: 8:26:14 AM
 */
public abstract class HBaseSchema extends Schema {

    private ColumnAttrib keyAttrib = null;

    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> versionAttribByFamilyQualifiedNameMap = Maps.newHashMap();
    private final Map<String, List<ColumnAttrib>> columnAttribListByFamilyNameMap = Maps.newHashMap();

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return null;
    }

    public ColumnAttrib getKeyAttrib() {
        return this.keyAttrib;
    }

    protected void setKeyAttrib(final ColumnAttrib keyAttrib) {
        this.keyAttrib = keyAttrib;
    }

    public abstract String getSchemaName();

    public abstract String getTableName();

    public abstract List<HColumnDescriptor> getColumnDescriptors();

    public byte[] getTableNameAsBytes() throws IOException, HPersistException {
        return HUtil.ser.getStringAsBytes(this.getTableName());
    }

    public abstract Object getObject(final Serialization ser,
                                     final List<String> fieldList,
                                     final int maxVersions,
                                     final Result result) throws HPersistException;


    public static HBaseSchema findSchema(final String tablename) throws HPersistException {

        // First look in defined schema, then try annotation schema
        HBaseSchema schema;

        schema = DefinedSchema.getDefinedSchema(tablename);
        if (schema != null)
            return schema;

        schema = AnnotationSchema.getAnnotationSchema(tablename);
        if (schema != null)
            return schema;

        throw new HPersistException("Unknown table: " + tablename);
    }

    // *** columnAttribByFamilyQualifiedNameMap calls
    protected Map<String, ColumnAttrib> getColumnAttribByFamilyQualifiedNameMap() {
        return this.columnAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getColumnAttribFromFamilyQualifiedNameMap(final String familyName, final String columnName) {
        return this.getColumnAttribByFamilyQualifiedNameMap().get(familyName + ":" + columnName);
    }

    protected void addColumnAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HPersistException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getColumnAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");
        this.getColumnAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** versionAttribByFamilyQualifiedNameMap calls
    private Map<String, ColumnAttrib> getVersionAttribByFamilyQualifiedNameMap() {
        return versionAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getVersionAttribFromFamilyQualifiedNameMap(final String s) {
        return this.getVersionAttribByFamilyQualifiedNameMap().get(s);
    }

    protected void addVersionAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HPersistException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getVersionAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");

        this.getVersionAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** columnAttribListByFamilyNameMap
    private Map<String, List<ColumnAttrib>> getColumnAttribListByFamilyNameMap() {
        return columnAttribListByFamilyNameMap;
    }

    public Set<String> getFamilySet() {
        return this.getColumnAttribListByFamilyNameMap().keySet();
    }

    public List<ColumnAttrib> getColumnAttribListByFamilyName(final String familyName) {
        return this.getColumnAttribListByFamilyNameMap().get(familyName);
    }

    protected boolean containsFamilyNameInFamilyNameMap(final String s) {
        return this.getColumnAttribListByFamilyNameMap().containsKey(s);
    }

    public void addColumnAttribListFamilyNameMap(final String familyName,
                                                 final List<ColumnAttrib> attribList) throws HPersistException {
        if (this.containsFamilyNameInFamilyNameMap(familyName))
            throw new HPersistException(familyName + " already delcared");
        this.getColumnAttribListByFamilyNameMap().put(familyName, attribList);
    }

    public void addColumnAttribListToFamilyNameMap(ColumnAttrib attrib) throws HPersistException {

        if (attrib.isKeyAttrib())
            return;

        final String familyName = attrib.getFamilyName();

        if (familyName == null || familyName.length() == 0)
            return;

        final List<ColumnAttrib> attribList;
        if (!this.containsFamilyNameInFamilyNameMap(familyName)) {
            attribList = Lists.newArrayList();
            this.getColumnAttribListByFamilyNameMap().put(familyName, attribList);
        }
        else {
            attribList = this.getColumnAttribListByFamilyName(familyName);
        }

        attribList.add(attrib);
    }


    protected void assignCurrentValues(final Serialization ser,
                                       final List<String> fieldList,
                                       final Result result,
                                       final Object newobj) throws IOException, HPersistException {

        for (final KeyValue keyValue : result.list()) {

            final byte[] familyBytes = keyValue.getFamily();
            final byte[] columnBytes = keyValue.getQualifier();
            final String familyName = ser.getStringFromBytes(familyBytes);
            final String columnName = ser.getStringFromBytes(columnBytes);
            final long timestamp = keyValue.getTimestamp();
            final byte[] valueBytes = result.getValue(familyBytes, columnBytes);

            if (columnName.endsWith("]")) {
                final int lbrace = columnName.indexOf("[");
                final String mapcolumn = columnName.substring(0, lbrace);
                final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                final ColumnAttrib attrib = this.getColumnAttribFromFamilyQualifiedNameMap(familyName, mapcolumn);

                Map mapval = (Map)attrib.getCurrentValue(newobj);

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    // TODO Check this
                    attrib.setVersionedValueMap(newobj, mapval);
                }

                final Object val = attrib.getValueFromBytes(ser, newobj, valueBytes);
                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = this.getColumnAttribFromFamilyQualifiedNameMap(familyName, columnName);
                attrib.setCurrentValue(ser, newobj, timestamp, valueBytes);
            }
        }
    }

    protected void assignVersionedValues(final Serialization ser,
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

                    final ColumnAttrib attrib = this.getVersionAttribFromFamilyQualifiedNameMap(qualifiedName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!fieldList.contains(attrib.getVariableName()))
                        continue;

                    Map<Long, Object> mapval = (Map<Long, Object>)attrib.getVersionedValueMap(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setVersionedValueMap(newobj, mapval);
                    }

                    final Object val = attrib.getValueFromBytes(ser, newobj, vbytes);
                    mapval.put(timestamp, val);
                }
            }
        }
    }

    // This is relevant only for AnnotatedSchema
    public List<VarDesc> getVarDescList() {
        return null;
    }

    public List<Scan> getScanList(final List<String> fieldList,
                                  final KeyRangeArgs keyRangeArgs,
                                  final DateRangeArgs dateRangeArgs,
                                  final VersionArgs versionArgs,
                                  final HBqlFilter serverFilter) throws IOException, HPersistException {

        final List<Scan> scanList = Lists.newArrayList();
        final List<KeyRangeArgs.Range> rangeList = keyRangeArgs.getRangeList();

        if (rangeList.size() == 0) {
            scanList.add(new Scan());
        }
        else {
            for (final KeyRangeArgs.Range range : rangeList) {
                final Scan scan = new Scan();
                if (range.isRecordRange()) {
                    scan.setStartRow(range.getLowerAsBytes());
                    if (!range.isStartKeyOnly())
                        scan.setStopRow(range.getUpperAsBytes());
                    scanList.add(scan);
                }
            }
        }

        for (final Scan scan : scanList) {

            // Set column names
            for (final String name : fieldList) {

                final ColumnAttrib attrib = (ColumnAttrib)this.getVariableAttribByVariableName(name);

                if (attrib == null)
                    throw new HPersistException("Column " + name + " does not exist in " + this.getSchemaName());

                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyNameAsBytes());
                else
                    scan.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
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

            if (serverFilter != null)
                scan.setFilter(serverFilter);
        }

        return scanList;
    }

    public HBqlFilter getHBqlFilter(final ExprTree exprTree,
                                    final List<String> fieldList,
                                    final long scanLimit) throws HPersistException {

        if (!exprTree.isValid())
            return (scanLimit > 0) ? new HBqlFilter(ExprTree.newExprTree(null), scanLimit) : null;
        else
            return new HBqlFilter(exprTree.setSchema(HUtil.getServerSchema(this), fieldList), scanLimit);
    }

}
