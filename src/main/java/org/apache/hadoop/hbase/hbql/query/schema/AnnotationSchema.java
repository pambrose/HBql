package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HColumn;
import org.apache.hadoop.hbase.hbql.client.HColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.HFamily;
import org.apache.hadoop.hbase.hbql.client.HTable;
import org.apache.hadoop.hbase.hbql.query.stmt.select.SelectElement;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class AnnotationSchema extends HBaseSchema {

    private final static Map<Class<?>, AnnotationSchema> annotationSchemaMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private DefinedSchema defineSchemaEquiv = null;

    private AnnotationSchema(final Class clazz) throws HBqlException {

        this.clazz = clazz;

        // Make sure there is an empty constructor declared
        try {
            this.getClazz().getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new HBqlException("Class " + this + " is missing a null constructor");
        }

        this.table = this.getClazz().getAnnotation(HTable.class);

        if (this.table == null)
            throw new HBqlException("Class " + this + " is missing @HTable annotation");

        this.families = this.table.families();

        if (this.families == null)
            throw new HBqlException("Class " + this + " is missing @HFamily values in @HTable annotation");

        for (final HFamily family : families) {
            final List<ColumnAttrib> attribs = Lists.newArrayList();
            this.addAttribToFamilyNameColumnListMap(family.name(), attribs);
        }

        // First process all HColumn fields so we can do lookup from HColumnVersionMaps
        for (final Field field : this.getClazz().getDeclaredFields())
            if (field.getAnnotation(HColumn.class) != null)
                this.processColumnAnnotation(field);

        if (this.getKeyAttrib() == null)
            throw new HBqlException("Class " + this + " is missing an instance variable "
                                    + "annotated with @HColumn(key=true)");

        if (this.getKeyAttrib().getFamilyName().length() > 0)
            throw new HBqlException(this.getKeyAttrib().getNameToUseInExceptions() + " @HColumn annotation " +
                                    "cannot have a family name.");

        for (final Field field : this.getClazz().getDeclaredFields())
            if (field.getAnnotation(HColumnVersionMap.class) != null)
                this.processColumnVersionAnnotation(field);
    }

    public synchronized static AnnotationSchema getAnnotationSchema(final String objName) throws HBqlException {

        // First see if already cached
        Class<?> clazz = getClassCacheMap().get(objName);

        if (clazz != null)
            return getAnnotationSchema(clazz);

        final String classpath = System.getProperty("java.class.path");
        for (final String val : classpath.split(":")) {
            try {
                if (val.toLowerCase().endsWith(".jar")) {
                    JarFile jarFile = new JarFile(val);
                    Enumeration<JarEntry> entries = jarFile.entries();
                    while (entries.hasMoreElements()) {
                        final JarEntry entry = entries.nextElement();
                        final AnnotationSchema schema = searchPackage(entry.getName(), objName);
                        if (schema != null)
                            return schema;
                    }
                }

                final File rootdir = new File(val);
                if (rootdir.isDirectory())
                    return searchDirectory(rootdir, rootdir.getCanonicalPath(), "", objName + ".class");
            }
            catch (IOException e) {
                // Go to next entry
            }
        }

        return null;
    }

    private static AnnotationSchema searchDirectory(final File dir,
                                                    final String rootDir,
                                                    final String prefix,
                                                    final String dotClassName) throws HBqlException {

        final List<File> subdirs = Lists.newArrayList();
        final String[] contents = dir.list();

        for (final String elem : contents) {

            final File file = new File(dir + "/" + elem);
            if (file.isDirectory()) {
                subdirs.add(file);
            }
            else {
                final String pathname = file.getAbsolutePath().replace('/', '.');
                if (pathname.endsWith(dotClassName)) {
                    final String rootprefix = rootDir.replace('/', '.');
                    final String pathsuffix = pathname.substring(rootprefix.length() + 1);
                    return setClassCache(stripDotClass(pathsuffix), stripDotClass(dotClassName));
                }
            }
        }

        // Now search the dirs
        for (final File subdir : subdirs) {
            final String nextdir = (prefix.length() == 0) ? subdir.getName() : prefix + "." + subdir.getName();
            final AnnotationSchema schema = searchDirectory(subdir, rootDir, nextdir, dotClassName);
            if (schema != null)
                return schema;
        }

        return null;
    }

    private static String stripDotClass(final String str) {
        return str.substring(0, str.length() - ".class".length());
    }

    private static AnnotationSchema searchPackage(final String pkgName, final String objName) throws HBqlException {

        if (pkgName == null)
            return null;

        final String prefix = pkgName.replaceAll("/", ".");

        if (prefix.startsWith("META-INF.")
            || prefix.startsWith("antlr.")
            || prefix.startsWith("org.antlr.")
            || prefix.startsWith("apple.")
            || prefix.startsWith("com.apple.")
            || prefix.startsWith("sun.")
            || prefix.startsWith("com.sun.")
            || prefix.startsWith("java.")
            || prefix.startsWith("javax.")
            || prefix.startsWith("org.apache.zookeeper.")
            || prefix.startsWith("org.apache.bookkeeper.")
            || prefix.startsWith("org.apache.jute.")
            || prefix.startsWith("com.google.common.")
            || prefix.startsWith("org.apache.log4j.")
            || prefix.startsWith("junit.")
            || prefix.startsWith("org.junit.")
            || prefix.startsWith("org.xml.")
            || prefix.startsWith("org.w3c.")
            || prefix.startsWith("org.omg.")
            || prefix.startsWith("org.apache.mina.")
            || prefix.startsWith("org.apache.hadoop.")
            || prefix.startsWith("org.apache.commons.logging.")
            || prefix.startsWith("org.jcp.")
            || prefix.startsWith("org.slf4j.")
            || prefix.startsWith("org.ietf.")
            || prefix.startsWith("org.relaxng.")
            || prefix.startsWith("netscape.")
                )
            return null;

        final String fullname = prefix
                                + ((!prefix.endsWith(".") && prefix.length() > 0) ? "." : "")
                                + objName;

        return setClassCache(fullname, objName);
    }

    private static AnnotationSchema setClassCache(final String name, final String objName) throws HBqlException {

        final Class<?> clazz = getClass(name);
        if (clazz == null) {
            return null;
        }
        else {
            getClassCacheMap().put(objName, clazz);
            return getAnnotationSchema(clazz);
        }
    }

    private static Class getClass(final String str) throws HBqlException {
        try {
            final Class<?> clazz = Class.forName(str);

            // Make sure inner class is static
            if (clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers())) {
                final String s = "Inner class " + clazz.getName() + " must be declared static";
                System.err.println(s);
                throw new HBqlException(s);
            }

            return clazz;
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    public static AnnotationSchema getAnnotationSchema(final Object obj) throws HBqlException {
        return getAnnotationSchema(obj.getClass());
    }

    public synchronized static AnnotationSchema getAnnotationSchema(final Class<?> clazz) throws HBqlException {

        AnnotationSchema schema = getAnnotationSchemaMap().get(clazz);
        if (schema != null)
            return schema;

        schema = new AnnotationSchema(clazz);
        getAnnotationSchemaMap().put(clazz, schema);
        return schema;
    }

    public synchronized DefinedSchema getDefinedSchemaEquivalent() throws HBqlException {
        if (this.defineSchemaEquiv == null)
            this.defineSchemaEquiv = new DefinedSchema(this.getTableName(), null, this.getColumnDescriptionList());
        return this.defineSchemaEquiv;
    }

    private static Map<Class<?>, AnnotationSchema> getAnnotationSchemaMap() {
        return annotationSchemaMap;
    }

    private static Map<String, Class<?>> getClassCacheMap() {
        return classCacheMap;
    }

    public String toString() {
        return this.getSchemaName();
    }

    public String getSchemaName() {
        return this.getClazz().getName();
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    private HFamily[] getFamilies() {
        return this.families;
    }

    public List<HColumnDescriptor> getColumnDescriptors() {
        final List<HColumnDescriptor> descList = Lists.newArrayList();
        for (final HFamily family : this.getFamilies()) {
            final HColumnDescriptor columnDesc = new HColumnDescriptor(family.name());
            if (family.maxVersions() > 0)
                columnDesc.setMaxVersions(family.maxVersions());
            descList.add(columnDesc);
        }
        return descList;
    }

    private void processColumnAnnotation(final Field field) throws HBqlException {

        final CurrentValueAnnotationAttrib attrib = new CurrentValueAnnotationAttrib(field);

        this.addAttribToVariableNameMap(attrib, attrib.getAliasName());
        this.addAttribToFamilyQualifiedNameMap(attrib);
        this.addFamilyDefaultAttrib(attrib);

        if (attrib.isKeyAttrib()) {
            if (this.getKeyAttrib() != null)
                throw new HBqlException("Class " + this + " has multiple instance variables "
                                        + "annotated with @HColumn(key=true)");

            this.setKeyAttrib(attrib);
        }
        else {
            final String familyName = attrib.getFamilyName();

            if (familyName.length() == 0)
                throw new HBqlException(attrib.getObjectQualifiedName() + " is missing family name in annotation");

            if (!this.containsFamilyNameInFamilyNameMap(familyName))
                throw new HBqlException(attrib.getObjectQualifiedName() + " references unknown family: " + familyName);

            if (attrib.isFamilyDefaultAttrib()) {

                if (attrib.isMapKeysAsColumnsAttrib())
                    throw new HBqlException(attrib.getObjectQualifiedName()
                                            + " cannot have both mapKeysAsColumns and familyDefault marked as true");

                if (attrib.getColumnName() != null || attrib.getColumnName().length() > 0)
                    throw new HBqlException(attrib.getObjectQualifiedName()
                                            + " cannot have both a columnName and familyDefault marked as true");

                if (attrib.getGetter() != null || attrib.getGetter().length() > 0)
                    throw new HBqlException(attrib.getObjectQualifiedName()
                                            + " cannot have both a getter and familyDefault marked as true");

                if (attrib.getGetter() != null || attrib.getGetter().length() > 0)
                    throw new HBqlException(attrib.getObjectQualifiedName()
                                            + " cannot have both a getter and familyDefault marked as true");

                if (attrib.getSetter() != null || attrib.getSetter().length() > 0)
                    throw new HBqlException(attrib.getObjectQualifiedName()
                                            + " cannot have both a setter and familyDefault marked as true");
            }

            this.getColumnAttribListByFamilyName(familyName).add(attrib);
        }
    }

    private void processColumnVersionAnnotation(final Field field) throws HBqlException {

        final VersionAnnotationAttrib attrib = VersionAnnotationAttrib.newVersionAttrib(this, field);
        final String aliasName = attrib.getAliasName();

        this.addAttribToVariableNameMap(attrib, aliasName);
        this.addVersionAttrib(attrib);
        this.addFamilyDefaultAttrib(attrib);
    }

    public String getTableName() {
        final String tableName = this.table.name();
        return (tableName.length() > 0) ? tableName : clazz.getSimpleName();
    }

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return this.getClazz().newInstance();
    }

    public Object newObject(final List<ColumnAttrib> selectAttribList,
                            final List<SelectElement> selectElementList,
                            final int maxVersions,
                            final Result result) throws HBqlException {

        try {
            // Create object and assign values
            final Object newobj = this.createNewObject(result);
            this.assignSelectValues(newobj, selectAttribList, selectElementList, maxVersions, result);
            return newobj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HBqlException("Error in newObject() " + e.getMessage());
        }
    }

    private Object createNewObject(final Result result) throws HBqlException {

        // Create new instance and set key value
        final Object newobj;
        try {
            newobj = this.newInstance();
            this.getKeyAttrib().setCurrentValue(newobj, 0, result.getRow());
        }
        catch (InstantiationException e) {
            e.printStackTrace();
            throw new HBqlException("Cannot create new instance of " + this.getSchemaName());
        }
        catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new HBqlException("Cannot set value for key  " + this.getSchemaName()
                                    + "." + this.getKeyAttrib().getFamilyQualifiedName());
        }
        return newobj;
    }

    private List<ColumnDescription> getColumnDescriptionList() {
        final List<ColumnDescription> varList = Lists.newArrayList();
        for (final ColumnAttrib columnAttrib : this.getAttribByFamilyQualifiedNameMap().values()) {
            final String columnType = columnAttrib.isKeyAttrib()
                                      ? FieldType.KeyType.getFirstSynonym()
                                      : columnAttrib.getFieldType().getFirstSynonym();
            varList.add(ColumnDescription.newColumn(columnAttrib.getFamilyQualifiedName(),
                                                    columnAttrib.getAliasName(),
                                                    columnAttrib.isMapKeysAsColumnsAttrib(),
                                                    columnAttrib.isFamilyDefaultAttrib(),
                                                    columnType,
                                                    columnAttrib.isArray()));
        }
        return varList;
    }
}
