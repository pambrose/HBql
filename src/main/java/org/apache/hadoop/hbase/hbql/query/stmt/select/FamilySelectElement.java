package org.apache.hadoop.hbase.hbql.query.stmt.select;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.FamilyAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 1, 2009
 * Time: 8:40:58 PM
 */
public class FamilySelectElement implements SelectElement {

    private final boolean useAllFamilies;
    private final List<String> familyNameList = Lists.newArrayList();
    private final List<byte[]> familyNameBytesList = Lists.newArrayList();

    private HBaseSchema schema;

    public FamilySelectElement(final String familyName) {
        if (familyName != null && familyName.equals("*")) {
            this.useAllFamilies = true;
        }
        else {
            this.useAllFamilies = false;
            this.addAFamily(familyName.replace(" ", "").replace(":*", ""));
        }
    }

    public void addAFamily(final String familyName) {
        this.familyNameList.add(familyName);
    }

    public static SelectElement newAllFamilies() {
        return new FamilySelectElement("*");
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

    private HBaseSchema getSchema() {
        return this.schema;
    }

    @Override
    public void validate(final HBaseSchema schema, final List<ColumnAttrib> selectAttribList) throws HBqlException {

        this.schema = schema;

        if (this.useAllFamilies) {
            for (final String familyName : schema.getFamilySet()) {
                this.addAFamily(familyName);
                selectAttribList.add(new FamilyAttrib(familyName));
            }
        }
        else {
            // Only has one family
            final String familyName = this.getFamilyNameList().get(0);
            if (!schema.containsFamilyNameInFamilyNameMap(familyName))
                throw new HBqlException("Invalid family name: " + familyName);

            selectAttribList.add(new FamilyAttrib(familyName));
        }

        for (final String familyName : this.getFamilyNameList())
            this.getFamilyNameBytesList().add(HUtil.ser.getStringAsBytes(familyName));
    }

    @Override
    public void assignCurrentValue(final Object newobj, final Result result) throws HBqlException {

        // Evaluate each of the families
        for (int i = 0; i < this.getFamilyNameBytesList().size(); i++) {
            final String familyName = this.getFamilyNameList().get(i);
            final byte[] familyNameBytes = this.getFamilyNameBytesList().get(i);

            final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(familyNameBytes);

            for (final byte[] columnBytes : columnMap.keySet()) {

                final String columnName = HUtil.ser.getStringFromBytes(columnBytes);
                final byte[] b = columnMap.get(columnBytes);

                if (columnName.endsWith("]")) {
                    final int lbrace = columnName.indexOf("[");
                    final String mapcolumn = columnName.substring(0, lbrace);
                    final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                    final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(familyName,
                                                                                                  mapcolumn);

                    if (attrib != null) {
                        Map mapval = (Map)attrib.getCurrentValue(newobj);

                        if (mapval == null) {
                            mapval = Maps.newHashMap();
                            // TODO Check this
                            attrib.setMapValue(newobj, mapval);
                        }

                        final Object val = attrib.getValueFromBytes(newobj, b);
                        mapval.put(mapKey, val);
                    }
                }
                else {
                    final ColumnAttrib attrib = this.getSchema()
                            .getAttribFromFamilyQualifiedName(familyName, columnName);
                    if (attrib != null)
                        attrib.setCurrentValue(newobj, 0, b);
                }

            }
        }
    }

    @Override
    public void assignVersionValue(final Object newobj, final Result result) throws HBqlException {

        // Evaluate each of the families
        for (int i = 0; i < this.getFamilyNameBytesList().size(); i++) {
            final String familyName = this.getFamilyNameList().get(i);
            final byte[] familyNameBytes = this.getFamilyNameBytesList().get(i);

            final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(familyNameBytes);

            for (final byte[] columnBytes : columnMap.keySet()) {

                final String columnName = HUtil.ser.getStringFromBytes(columnBytes);
                final byte[] b = columnMap.get(columnBytes);

                if (columnName.endsWith("]")) {
                    final int lbrace = columnName.indexOf("[");
                    final String mapcolumn = columnName.substring(0, lbrace);
                    final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                    final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(familyName,
                                                                                                  mapcolumn);

                    if (attrib != null) {
                        Map mapval = (Map)attrib.getCurrentValue(newobj);

                        if (mapval == null) {
                            mapval = Maps.newHashMap();
                            // TODO Check this
                            attrib.setMapValue(newobj, mapval);
                        }

                        final Object val = attrib.getValueFromBytes(newobj, b);
                        mapval.put(mapKey, val);
                    }
                }
                else {
                    final ColumnAttrib attrib = this.getSchema()
                            .getAttribFromFamilyQualifiedName(familyName, columnName);
                    if (attrib != null)
                        attrib.setCurrentValue(newobj, 0, b);
                }

            }
        }
    }
}
