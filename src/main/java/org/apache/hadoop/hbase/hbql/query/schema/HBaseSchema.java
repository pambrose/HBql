package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.antlr.args.KeyRangeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.SelectElement;
import org.apache.hadoop.hbase.hbql.query.antlr.args.TimeRangeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.VersionArgs;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.IOException;
import java.util.Collection;
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

    public String getTableAliasName() {
        return this.getTableName();
    }

    public abstract List<HColumnDescriptor> getColumnDescriptors();

    public byte[] getTableNameAsBytes() throws HBqlException {
        return HUtil.ser.getStringAsBytes(this.getTableName());
    }

    public abstract Object newObject(final Collection<ColumnAttrib> attribList,
                                     final List<SelectElement> selectElementList,
                                     final int maxVersions,
                                     final Result result) throws HBqlException;


    public static HBaseSchema findSchema(final String tablename) throws HBqlException {

        // First look in defined schema, then try annotation schema
        HBaseSchema schema = DefinedSchema.getDefinedSchema(tablename);
        if (schema != null)
            return schema;

        schema = AnnotationSchema.getAnnotationSchema(tablename);
        if (schema != null)
            return schema;

        throw new HBqlException("Unknown table: " + tablename);
    }

    // *** columnAttribByFamilyQualifiedNameMap calls
    protected Map<String, ColumnAttrib> getAttribByFamilyQualifiedNameMap() {
        return this.columnAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getAttribFromFamilyQualifiedName(final String familyName, final String columnName) {
        return this.getAttribFromFamilyQualifiedName(familyName + ":" + columnName);
    }

    public ColumnAttrib getAttribFromFamilyQualifiedName(final String familyQualifiedName) {
        return this.getAttribByFamilyQualifiedNameMap().get(familyQualifiedName);
    }

    protected void addAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HBqlException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HBqlException(name + " already declared");
        this.getAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** versionAttribByFamilyQualifiedNameMap calls
    private Map<String, ColumnAttrib> getVersionAttribByFamilyQualifiedNameMap() {
        return this.versionAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getVersionAttribFromFamilyQualifiedNameMap(final String qualifiedFamilyName) {
        return this.getVersionAttribByFamilyQualifiedNameMap().get(qualifiedFamilyName);
    }

    public ColumnAttrib getVersionAttribFromFamilyQualifiedNameMap(final String familyName, final String columnName) {
        return this.getVersionAttribFromFamilyQualifiedNameMap(familyName + ":" + columnName);
    }

    protected void addVersionAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HBqlException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getVersionAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HBqlException(name + " already declared");

        this.getVersionAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** columnAttribListByFamilyNameMap
    private Map<String, List<ColumnAttrib>> getColumnAttribListByFamilyNameMap() {
        return this.columnAttribListByFamilyNameMap;
    }

    public Set<String> getFamilySet() {
        return this.getColumnAttribListByFamilyNameMap().keySet();
    }

    public List<ColumnAttrib> getColumnAttribListByFamilyName(final String familyName) {
        return this.getColumnAttribListByFamilyNameMap().get(familyName);
    }

    public boolean containsFamilyNameInFamilyNameMap(final String familyName) {
        return this.getColumnAttribListByFamilyNameMap().containsKey(familyName);
    }

    public void addColumnAttribListFamilyNameMap(final String familyName,
                                                 final List<ColumnAttrib> attribList) throws HBqlException {
        if (this.containsFamilyNameInFamilyNameMap(familyName))
            throw new HBqlException(familyName + " already declared");
        this.getColumnAttribListByFamilyNameMap().put(familyName, attribList);
    }

    public void addColumnAttribListToFamilyNameMap(ColumnAttrib attrib) throws HBqlException {

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


    protected void assignCurrentValuesFromExpr(final Object newobj,
                                               final List<SelectElement> selectElementList,
                                               final Result result) throws HBqlException {

        for (final SelectElement selectElement : selectElementList)
            selectElement.assignCurrentValue(newobj, result);
    }

    protected void assignCurrentValuesFromResult(final Object newobj, final Result result) throws HBqlException {

        for (final KeyValue keyValue : result.list()) {

            final byte[] fbytes = keyValue.getFamily();
            final byte[] cbytes = keyValue.getQualifier();

            final String familyName = HUtil.ser.getStringFromBytes(fbytes);
            final String columnName = HUtil.ser.getStringFromBytes(cbytes);

            final long timestamp = keyValue.getTimestamp();
            final byte[] b = result.getValue(fbytes, cbytes);

            if (columnName.endsWith("]")) {
                final int lbrace = columnName.indexOf("[");
                final String mapcolumn = columnName.substring(0, lbrace);
                final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                final ColumnAttrib attrib = this.getAttribFromFamilyQualifiedName(familyName, mapcolumn);

                Map mapval = (Map)attrib.getCurrentValue(newobj);

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    // TODO Check this
                    attrib.setMapValue(newobj, mapval);
                }

                final Object val = attrib.getValueFromBytes(newobj, b);
                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = this.getAttribFromFamilyQualifiedName(familyName, columnName);
                attrib.setCurrentValue(newobj, timestamp, b);
            }
        }
    }

    protected void assignVersionedValuesFromExpr(final Object newobj,
                                                 final List<SelectElement> selectElementList,
                                                 final Collection<ColumnAttrib> columnAttribs,
                                                 final Result result) throws HBqlException {

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

        for (final byte[] fbytes : familyMap.keySet()) {

            final String familyName = HUtil.ser.getStringFromBytes(fbytes);
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(fbytes);

            for (final byte[] cbytes : columnMap.keySet()) {

                final String columnName = HUtil.ser.getStringFromBytes(cbytes);
                final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(cbytes);

                for (final Long timestamp : timeStampMap.keySet()) {

                    final ColumnAttrib attrib = this.getVersionAttribFromFamilyQualifiedNameMap(familyName, columnName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!columnAttribs.contains(attrib))
                        continue;

                    Map<Long, Object> mapval = (Map<Long, Object>)attrib.getMapValue(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setMapValue(newobj, mapval);
                    }

                    final byte[] b = timeStampMap.get(timestamp);
                    final Object val = attrib.getValueFromBytes(newobj, b);
                    mapval.put(timestamp, val);
                }
            }
        }
    }

    protected void assignVersionedValuesFromResult(final Object newobj,
                                                   final Collection<ColumnAttrib> columnAttribs,
                                                   final Result result) throws HBqlException {

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

        for (final byte[] fbytes : familyMap.keySet()) {

            final String familyName = HUtil.ser.getStringFromBytes(fbytes);
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(fbytes);

            for (final byte[] cbytes : columnMap.keySet()) {
                final String columnName = HUtil.ser.getStringFromBytes(cbytes);
                final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(cbytes);

                for (final Long timestamp : timeStampMap.keySet()) {

                    final ColumnAttrib attrib = this.getVersionAttribFromFamilyQualifiedNameMap(familyName, columnName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!columnAttribs.contains(attrib))
                        continue;

                    Map<Long, Object> mapval = (Map<Long, Object>)attrib.getMapValue(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setMapValue(newobj, mapval);
                    }

                    final byte[] b = timeStampMap.get(timestamp);
                    final Object val = attrib.getValueFromBytes(newobj, b);
                    mapval.put(timestamp, val);
                }
            }
        }
    }

    // This is relevant only for AnnotatedSchema
    public List<ColumnDescription> getColumnDescriptionList() {
        return null;
    }

    public List<Scan> getScanList(final Collection<ColumnAttrib> columnAttribSet,
                                  final KeyRangeArgs keyRangeArgs,
                                  final TimeRangeArgs timeRangeArgs,
                                  final VersionArgs versionArgs,
                                  final HBqlFilter serverFilter) throws IOException, HBqlException {

        final List<Scan> scanList = Lists.newArrayList();
        final List<KeyRangeArgs.Range> rangeList = keyRangeArgs.getRangeList();

        if (rangeList.size() == 0) {
            scanList.add(new Scan());
        }
        else {
            for (final KeyRangeArgs.Range range : rangeList) {
                final Scan scan = new Scan();
                scan.setStartRow(range.getLowerAsBytes());
                if (!range.isStartLastRange())
                    scan.setStopRow(range.getUpperAsBytes());
                scanList.add(scan);
            }
        }

        for (final Scan scan : scanList) {

            // Set column names
            for (final ColumnAttrib attrib : columnAttribSet) {

                // Do not bother to request because it will always be delivered
                if (attrib.isKeyAttrib())
                    continue;

                // If it is a map, then request all columns for family
                if (attrib.isAFamilyAttrib() || attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyNameAsBytes());
                else
                    scan.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
            }

            if (timeRangeArgs != null && timeRangeArgs.isValid()) {
                if (timeRangeArgs.getLower() == timeRangeArgs.getUpper())
                    scan.setTimeStamp(timeRangeArgs.getLower());
                else
                    scan.setTimeRange(timeRangeArgs.getLower(), timeRangeArgs.getUpper());
            }

            if (versionArgs != null && versionArgs.isValid()) {
                final int max = versionArgs.getValue();
                if (max == Integer.MAX_VALUE)
                    scan.setMaxVersions();
                else
                    scan.setMaxVersions(max);
            }

            if (serverFilter != null)
                scan.setFilter(serverFilter);
        }

        return scanList;
    }

    public HBqlFilter getHBqlFilter(final ExprTree exprTree, final long scanLimit) throws HBqlException {

        if (!exprTree.isValid())
            return (scanLimit > 0) ? new HBqlFilter(ExprTree.newExprTree(null), scanLimit) : null;

        final DefinedSchema schema = HUtil.getDefinedSchemaForServerFilter(this);
        exprTree.setSchema(schema);
        exprTree.validate();
        return new HBqlFilter(exprTree, scanLimit);
    }

}
