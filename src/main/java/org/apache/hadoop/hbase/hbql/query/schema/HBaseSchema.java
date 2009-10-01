package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.antlr.args.KeyRangeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.TimeRangeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.VersionArgs;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
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

    public String getAliasName() {
        return this.getTableName();
    }

    public abstract List<HColumnDescriptor> getColumnDescriptors();

    public byte[] getTableNameAsBytes() throws IOException, HBqlException {
        return HUtil.ser.getStringAsBytes(this.getTableName());
    }

    public abstract Object newObject(final Set<ColumnAttrib> attribSet,
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
    protected Map<String, ColumnAttrib> getColumnAttribByFamilyQualifiedNameMap() {
        return this.columnAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getColumnAttribFromFamilyQualifiedNameMap(final String familyName, final String columnName) {
        return this.getColumnAttribByFamilyQualifiedNameMap().get(familyName + ":" + columnName);
    }

    protected void addColumnAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HBqlException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getColumnAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HBqlException(name + " already delcared");
        this.getColumnAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** versionAttribByFamilyQualifiedNameMap calls
    private Map<String, ColumnAttrib> getVersionAttribByFamilyQualifiedNameMap() {
        return this.versionAttribByFamilyQualifiedNameMap;
    }

    public ColumnAttrib getVersionAttribFromFamilyQualifiedNameMap(final String s) {
        return this.getVersionAttribByFamilyQualifiedNameMap().get(s);
    }

    protected void addVersionAttribToFamilyQualifiedNameMap(final ColumnAttrib attrib) throws HBqlException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getVersionAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HBqlException(name + " already delcared");

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

    public boolean containsFamilyNameInFamilyNameMap(final String s) {
        return this.getColumnAttribListByFamilyNameMap().containsKey(s);
    }

    public void addColumnAttribListFamilyNameMap(final String familyName,
                                                 final List<ColumnAttrib> attribList) throws HBqlException {
        if (this.containsFamilyNameInFamilyNameMap(familyName))
            throw new HBqlException(familyName + " already delcared");
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


    protected void assignCurrentValues(final Object newobj, final Result result) throws IOException, HBqlException {

        for (final KeyValue keyValue : result.list()) {

            final byte[] fbytes = keyValue.getFamily();
            final byte[] cbytes = keyValue.getQualifier();

            final String fname = HUtil.ser.getStringFromBytes(fbytes);
            final String cname = HUtil.ser.getStringFromBytes(cbytes);

            final long timestamp = keyValue.getTimestamp();
            final byte[] valueBytes = result.getValue(fbytes, cbytes);

            if (cname.endsWith("]")) {
                final int lbrace = cname.indexOf("[");
                final String mapcolumn = cname.substring(0, lbrace);
                final String mapKey = cname.substring(lbrace + 1, cname.length() - 1);
                final ColumnAttrib attrib = this.getColumnAttribFromFamilyQualifiedNameMap(fname, mapcolumn);

                Map mapval = (Map)attrib.getCurrentValue(newobj);

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    // TODO Check this
                    attrib.setVersionedValueMap(newobj, mapval);
                }

                final Object val = attrib.getValueFromBytes(newobj, valueBytes);
                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = this.getColumnAttribFromFamilyQualifiedNameMap(fname, cname);
                attrib.setCurrentValue(newobj, timestamp, valueBytes);
            }
        }
    }

    protected void assignVersionedValues(final Object newobj,
                                         final Result result,
                                         final Set<ColumnAttrib> attribSet) throws IOException, HBqlException {

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

        for (final byte[] fbytes : familyMap.keySet()) {

            final String fname = HUtil.ser.getStringFromBytes(fbytes) + ":";
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(fbytes);

            for (final byte[] cbytes : columnMap.keySet()) {
                final String cname = HUtil.ser.getStringFromBytes(cbytes);
                final String qualifiedName = fname + cname;
                final NavigableMap<Long, byte[]> tsMap = columnMap.get(cbytes);

                for (final Long timestamp : tsMap.keySet()) {
                    final byte[] vbytes = tsMap.get(timestamp);

                    final ColumnAttrib attrib = this.getVersionAttribFromFamilyQualifiedNameMap(qualifiedName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!attribSet.contains(attrib))
                        continue;

                    Map<Long, Object> mapval = (Map<Long, Object>)attrib.getVersionedValueMap(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setVersionedValueMap(newobj, mapval);
                    }

                    final Object val = attrib.getValueFromBytes(newobj, vbytes);
                    mapval.put(timestamp, val);
                }
            }
        }
    }

    // This is relevant only for AnnotatedSchema
    public List<ColumnDescription> getColumnDescriptionList() {
        return null;
    }

    public List<Scan> getScanList(final Set<ColumnAttrib> attribSet,
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
            for (final ColumnAttrib columnAttrib : attribSet) {

                final ColumnAttrib attrib = (ColumnAttrib)columnAttrib;

                // Do not bother to request because it will always be delivered
                if (attrib.isKeyAttrib())
                    continue;

                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
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

    public HBqlFilter getHBqlFilter(final ExprTree exprTree,
                                    final Set<ColumnAttrib> attribSet,
                                    final long scanLimit) throws HBqlException {

        if (!exprTree.isValid())
            return (scanLimit > 0) ? new HBqlFilter(ExprTree.newExprTree(true, null), scanLimit) : null;

        final DefinedSchema schema = HUtil.getDefinedSchemaForServerFilter(this);
        exprTree.setSchema(schema);
        exprTree.validate(attribSet);
        return new HBqlFilter(exprTree, scanLimit);
    }

}
