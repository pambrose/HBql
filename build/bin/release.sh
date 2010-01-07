#!/bin/bash

export MAVEN_OPTS="-Xms2048m -Xmx2048m -DskipTests=true"

export VERSION="0.9.19-alpha"
export DIST="hbql-"${VERSION}
export DOCS="javadocs-"${VERSION}
export HBQL="/Users/pambrose/git/hbql"
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

# build jar of sources
cd ${RELEASE}/src
jar cf ${RELEASE}/${DIST}-src.jar *
cd ..
rm -rf src

# build jar of classes
cd ${HBQL}/target/classes
jar cf ${RELEASE}/${DIST}.jar *

cp -r ${SITE}/* ${RELEASE}/docs/
mkdir ${SITE}/downloads

rm -rf ${RELEASE}/docs/cobertura

cd ${SITE}
zip -q -r ${SITE}/downloads/${DOCS}.zip apidocs

# Create downloads
cd ${RELEASE}/..
zip -q -r ${SITE}/downloads/${DIST}.zip ${DIST}
tar cf ${SITE}/downloads/${DIST}.tar ${DIST}
gzip --quiet ${SITE}/downloads/${DIST}.tar

cd ${HBQL}
mvn site:deploy
