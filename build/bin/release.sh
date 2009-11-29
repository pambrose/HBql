#! /bin/bash

export MAVEN_OPTS="-Xms1024m -Xmx1024m"

export VERSION="0.9.10-alpha"
export DIST="hbql-"${VERSION}
export DOCS="javadocs-"${VERSION}
export HBQL="/Users/pambrose/git/hbase-plugin"
export RELEASE=${HBQL}/target/release/${DIST}
export SITE=${HBQL}/target/site

mvn clean

mkdir -p ${RELEASE}
mkdir ${RELEASE}/docs
mkdir ${RELEASE}/src

cp ${HBQL}/LICENSE.txt ${RELEASE}
cp ${HBQL}/NOTICE.txt ${RELEASE}
cp -r ${HBQL}/lib ${RELEASE}
cp -r ${HBQL}/bin ${RELEASE}
cp -r ${HBQL}/src/main ${RELEASE}/src
cp -r ${HBQL}/src/test ${RELEASE}/src
rm -rf ${RELEASE}/src/main/antlr3/org/apache/hadoop/hbase/hbql/antlr/output

mvn antlr3:antlr
mvn compiler:compile
mvn javadoc:javadoc
mvn site:site

rm -rf ${SITE}/testapidocs
rm -rf ${SITE}/xref-test

cd ${HBQL}/target/classes
jar cf ${RELEASE}/${DIST}.jar *

mkdir ${SITE}/downloads
cp -r ${SITE}/* ${RELEASE}/docs/

rm -rf ${RELEASE}/docs/cobertura

cd ${SITE}
zip -q -r ${SITE}/downloads/${DOCS}.zip apidocs

cd ${RELEASE}/..
zip -q -r ${SITE}/downloads/${DIST}.zip ${DIST}
tar cf ${SITE}/downloads/${DIST}.tar ${DIST}
gzip --quiet ${SITE}/downloads/${DIST}.tar

cd ${HBQL}
mvn site:deploy
