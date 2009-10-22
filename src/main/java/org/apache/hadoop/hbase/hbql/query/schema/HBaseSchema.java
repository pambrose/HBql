package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.stmt.select.SelectElement;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class HBaseSchema extends Schema {

    private ColumnAttrib keyAttrib = null;

    private Set<String> familyNameSet = null;

    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> versionAttribMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> familyDefaultMap = Maps.newHashMap();
    private final Map<String, List<ColumnAttrib>> columnAttribListByFamilyNameMap = Maps.newHashMap();

    protected HBaseSchema(final String schemaName) {
        super(schemaName);
    }

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return null;
    }

    public ColumnAttrib getKeyAttrib() {
        return this.keyAttrib;
    }

    protected void setKeyAttrib(final ColumnAttrib keyAttrib) {
        this.keyAttrib = keyAttrib;
    }

    public abstract String getTableName();

    protected abstract DefinedSchema getDefinedSchemaEquivalent() throws HBqlException;

    public abstract List<HColumnDescriptor> getColumnDescriptors();

    public byte[] getTableNameAsBytes() throws HBqlException {
        return HUtil.getSerialization().getStringAsBytes(this.getTableName());
    }

    public abstract Object newObject(final List<ColumnAttrib> selectAttribList,
                                     final List<SelectElement> selectElementList,
                                     final int maxVersions,
                                     final Result result) throws HBqlException;


    public static HBaseSchema findSchema(final String tablename) throws HBqlException {

        // First look in defined schema, then try annotation schema
        HBaseSchema schema = SchemaManager.getDefinedSchema(tablename);
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

        if (attrib.isFamilyDefaultAttrib())
            return;

        final String name = attrib.getFamilyQualifiedName();
        if (this.getAttribByFamilyQualifiedNameMap().containsKey(name))
            throw new HBqlException(name + " already declared");
        this.getAttribByFamilyQualifiedNameMap().put(name, attrib);
    }

    // *** familyDefaultMap calls
    private Map<String, ColumnAttrib> getFamilyDefaultMap() {
        return this.familyDefaultMap;
    }

    public ColumnAttrib getFamilyDefault(final String name) {
        return this.getFamilyDefaultMap().get(name);
    }

    protected void addFamilyDefaultAttrib(final ColumnAttrib attrib) throws HBqlException {

        if (!attrib.isFamilyDefaultAttrib())
            return;

        final String familyName = attrib.getFamilyName();
        if (this.getFamilyDefaultMap().containsKey(familyName))
            throw new HBqlException(familyName + " already declared");

        this.getFamilyDefaultMap().put(familyName, attrib);

        final String aliasName = attrib.getAliasName();
        if (aliasName == null || aliasName.length() == 0 || aliasName.equals(familyName))
            return;

        if (this.getFamilyDefaultMap().containsKey(aliasName))
            throw new HBqlException(aliasName + " already declared");

        this.getFamilyDefaultMap().put(aliasName, attrib);
    }

    // *** versionAttribByFamilyQualifiedNameMap calls
    private Map<String, ColumnAttrib> getVersionAttribMap() {
        return this.versionAttribMap;
    }

    public ColumnAttrib getVersionAttrib(final String name) {
        return this.getVersionAttribMap().get(name);
    }

    public ColumnAttrib getVersionAttribMap(final String familyName, final String columnName) {
        return this.getVersionAttrib(familyName + ":" + columnName);
    }

    protected void addVersionAttrib(final ColumnAttrib attrib) throws HBqlException {

        if (!attrib.isAVersionValue())
            return;

        final String familyQualifiedName = attrib.getFamilyQualifiedName();
        if (this.getVersionAttribMap().containsKey(familyQualifiedName))
            throw new HBqlException(familyQualifiedName + " already declared");

        this.getVersionAttribMap().put(familyQualifiedName, attrib);
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

    public void addAttribToFamilyNameColumnListMap(final String familyName,
                                                   final List<ColumnAttrib> attribList) throws HBqlException {
        if (this.containsFamilyNameInFamilyNameMap(familyName))
            throw new HBqlException(familyName + " already declared");
        this.getColumnAttribListByFamilyNameMap().put(familyName, attribList);
    }

    public void addAttribToFamilyNameColumnListMap(ColumnAttrib attrib) throws HBqlException {

        if (attrib.isKeyAttrib() || attrib.isFamilyDefaultAttrib())
            return;

        final String familyName = attrib.getFamilyName();

        if (familyName == null || familyName.length() == 0)
            return;

        final List<ColumnAttrib> attribList;
        if (!this.containsFamilyNameInFamilyNameMap(familyName)) {
            attribList = Lists.newArrayList();
            this.addAttribToFamilyNameColumnListMap(familyName, attribList);
        }
        else {
            attribList = this.getColumnAttribListByFamilyName(familyName);
        }
        attribList.add(attrib);
    }

    protected void assignSelectValues(final Object newobj,
                                      final List<ColumnAttrib> selectAttribList,
                                      final List<SelectElement> selectElementList,
                                      final int maxVersions,
                                      final Result result) throws HBqlException {

        // Set key value
        this.getKeyAttrib().setCurrentValue(newobj, 0, result.getRow());

        for (final SelectElement selectElement : selectElementList)
            selectElement.assignValues(newobj, selectAttribList, maxVersions, result);
    }


    public HBqlFilter getHBqlFilter(final ExprTree origExprTree, final long scanLimit) throws HBqlException {

        if (origExprTree == null && scanLimit == 0)
            return null;

        final ExprTree exprTree = (origExprTree == null) ? ExprTree.newExprTree(true) : origExprTree;

        final DefinedSchema definedSchema = this.getDefinedSchemaEquivalent();
        exprTree.setSchema(definedSchema);
        return new HBqlFilter(exprTree, scanLimit);
    }

    public synchronized Set<String> getSchemaFamilyNames(final HConnection connection) throws HBqlException {

        // TODO May not want to cache this
        if (this.familyNameSet == null) {
            // Connction will be null from tests
            this.familyNameSet = (connection == null)
                                 ? this.getFamilySet()
                                 : connection.getFamilyList(this.getTableName());
        }

        return this.familyNameSet;
    }
}
