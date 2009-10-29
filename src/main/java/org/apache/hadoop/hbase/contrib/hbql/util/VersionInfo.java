package org.apache.hadoop.hbase.contrib.hbql.util;

import org.apache.hadoop.hbase.contrib.hbql.VersionAnnotation;


/**
 * This class finds the package info for hbase and the VersionAnnotation
 * information.  Taken from hadoop.  Only name of annotation is different.
 */
public class VersionInfo {
    private static Package myPackage;
    private static VersionAnnotation version;

    static {
        myPackage = VersionAnnotation.class.getPackage();
        version = myPackage.getAnnotation(VersionAnnotation.class);
    }

    /**
     * Get the meta-data for the hbase package.
     *
     * @return
     */
    static Package getPackage() {
        return myPackage;
    }

    /**
     * Get the hbase version.
     *
     * @return the hbase version string, eg. "0.6.3-dev"
     */
    public static String getVersion() {
        return version != null ? version.version() : "Unknown";
    }

    /**
     * Get the subversion revision number for the root directory
     *
     * @return the revision number, eg. "451451"
     */
    public static String getRevision() {
        return version != null ? version.revision() : "Unknown";
    }

    /**
     * The date that hbase was compiled.
     *
     * @return the compilation date in unix date format
     */
    public static String getDate() {
        return version != null ? version.date() : "Unknown";
    }

    /**
     * The user that compiled hbase.
     *
     * @return the username of the user
     */
    public static String getUser() {
        return version != null ? version.user() : "Unknown";
    }

    /**
     * Get the subversion URL for the root hbase directory.
     *
     * @return the url
     */
    public static String getUrl() {
        return version != null ? version.url() : "Unknown";
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("HBql " + getVersion());
    }
}
