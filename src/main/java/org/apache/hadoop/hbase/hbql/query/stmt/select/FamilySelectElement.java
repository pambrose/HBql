package org.apache.hadoop.hbase.hbql.query.stmt.select;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.SelectFamilyAttrib;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class FamilySelectElement implements SelectElement {

    private final boolean useAllFamilies;
    private final List<String> familyNameList = Lists.newArrayList();
    private final List<byte[]> familyNameBytesList = Lists.newArrayList();
    private final String familyName;

    private HBaseSchema schema;

    public FamilySelectElement(final String familyName) {

        this.familyName = familyName;

        if (familyName != null && familyName.equals("*")) {
            this.useAllFamilies = true;
        }
        else {
            this.useAllFamilies = false;
            this.addAFamily(familyName.replaceAll(" ", "").replace(":*", ""));
        }
    }

    public void addAFamily(final String familyName) {
        this.familyNameList.add(familyName);
    }

    public static List<SelectElement> newAllFamilies() {
        final List<SelectElement> retval = Lists.newArrayList();
        retval.add(new FamilySelectElement("*"));
        return retval;
    }

    public static FamilySelectElement newFamilyElement(final String family) {
        return new FamilySelectElement(family);
    }

    public List<String> getFamilyNameList() {
        return this.familyNameList;
    }

    public List<byte[]> getFamilyNameBytesList() {
        return this.familyNameBytesList;
    }

    protected HBaseSchema getSchema() {
        return this.schema;
    }

    public String getAsName() {
        return null;
    }

    public String asString() {
        return this.familyName;
    }

    public int setParameter(final String name, final Object val) {
        // Do nothing
        return 0;
    }

    public void validate(final HConnection connection,
                         final HBaseSchema schema,
                         final List<ColumnAttrib> selectAttribList) throws HBqlException {

        this.schema = schema;

        if (this.useAllFamilies) {
            // connction will be null from tests
            final Collection<String> familyList = this.getSchema().getSchemaFamilyNames(connection);
            for (final String familyName : familyList) {
                this.addAFamily(familyName);
                selectAttribList.add(new SelectFamilyAttrib(familyName));
            }
        }
        else {
            // Only has one family
            final String familyName = this.getFamilyNameList().get(0);
            if (!schema.containsFamilyNameInFamilyNameMap(familyName))
                throw new HBqlException("Invalid family name: " + familyName);

            selectAttribList.add(new SelectFamilyAttrib(familyName));
        }

        for (final String familyName : this.getFamilyNameList())
            this.getFamilyNameBytesList().add(HUtil.ser.getStringAsBytes(familyName));
    }

    public void assignValues(final Object obj,
                             final List<ColumnAttrib> selectAttribList,
                             final int maxVersions,
                             final Result result) throws HBqlException {

        final HBaseSchema schema = this.getSchema();

        // Evaluate each of the families (select * will yield all families)
        for (int i = 0; i < this.getFamilyNameBytesList().size(); i++) {

            final String familyName = this.getFamilyNameList().get(i);
            final byte[] familyNameBytes = this.getFamilyNameBytesList().get(i);

            final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(familyNameBytes);

            for (final byte[] columnBytes : columnMap.keySet()) {

                final byte[] valueBytes = columnMap.get(columnBytes);
                final String columnName = HUtil.ser.getStringFromBytes(columnBytes);

                if (columnName.endsWith("]")) {

                    final int lbrace = columnName.indexOf("[");
                    final String mapColumn = columnName.substring(0, lbrace);
                    final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                    final ColumnAttrib attrib = schema.getAttribFromFamilyQualifiedName(familyName, mapColumn);
                    if (attrib == null) {

                        final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(familyName);

                        if (familyDefaultAttrib != null)
                            familyDefaultAttrib.setFamilyDefaultKeysAsColumnsValue(obj, columnName, mapKey, valueBytes);

                        /*
                        // Set unknown attrib value to byte[] value
                        // Find value in results and assign the byte[] value to HRecord, but bail on Annotated object
                        if (!(newobj instanceof HRecord))
                            return;

                        ((HRecordImpl)newobj).setKeysAsColumnsValue(familyName + ":" + columnName,
                                                                           mapKey,
                                                                           0,
                                                                           currentValueBytes,
                                                                           false);
                                                                           */
                    }
                    else {
                        final Object val = attrib.getValueFromBytes(obj, valueBytes);
                        attrib.setKeysAsColumnsValue(obj, mapKey, val);
                    }
                }
                else {
                    final ColumnAttrib attrib = schema.getAttribFromFamilyQualifiedName(familyName, columnName);
                    if (attrib == null) {
                        final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(familyName);
                        if (familyDefaultAttrib != null)
                            familyDefaultAttrib.setFamilyDefaultCurrentValue(obj, columnName, valueBytes);
                    }
                    else {
                        attrib.setCurrentValue(obj, 0, valueBytes);
                    }
                }
            }

            // Bail if no versions were requested
            if (maxVersions <= 1)
                continue;

            final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> versionColumnMap = familyMap.get(familyNameBytes);

            if (versionColumnMap == null)
                continue;

            for (final byte[] columnBytes : versionColumnMap.keySet()) {

                final NavigableMap<Long, byte[]> timeStampMap = versionColumnMap.get(columnBytes);
                final String columnName = HUtil.ser.getStringFromBytes(columnBytes);

                if (columnName.endsWith("]")) {

                    final int lbrace = columnName.indexOf("[");
                    final String mapColumn = columnName.substring(0, lbrace);
                    final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);

                    final ColumnAttrib attrib = schema.getVersionAttribMap(familyName, mapColumn);

                    if (attrib == null) {
                        final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(familyName);
                        if (familyDefaultAttrib != null)
                            familyDefaultAttrib.setFamilyDefaultKeysAsColumnsVersionMap(obj,
                                                                                        columnName,
                                                                                        mapKey,
                                                                                        timeStampMap);
                    }
                    else {
                        // Set unknown attrib value to byte[] value
                        final Map<Long, Object> keysAsColumnsVersionMap = attrib.getKeysAsColumnsVersionMap(obj, mapKey);
                        for (final Long timestamp : timeStampMap.keySet()) {
                            final Object val = attrib.getValueFromBytes(obj, timeStampMap.get(timestamp));
                            keysAsColumnsVersionMap.put(timestamp, val);
                        }
                    }
                }
                else {
                    final ColumnAttrib attrib = schema.getVersionAttribMap(familyName, columnName);

                    if (attrib == null) {
                        final ColumnAttrib familyDefaultAttrib = schema.getFamilyDefault(familyName);
                        if (familyDefaultAttrib != null)
                            familyDefaultAttrib.setFamilyDefaultVersionMap(obj, columnName, timeStampMap);
                    }
                    else {
                        final Map<Long, Object> mapVal = attrib.getVersionMap(obj);

                        for (final Long timestamp : timeStampMap.keySet()) {
                            final byte[] b = timeStampMap.get(timestamp);
                            final Object val = attrib.getValueFromBytes(obj, b);
                            mapVal.put(timestamp, val);
                        }
                    }
                }
            }
        }
    }
}
