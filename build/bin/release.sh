#! /bin/bash

export MAVEN_OPTS="-Xms512m -Xmx512m"

export VERSION="0.9.6-alpha"
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

cd ${HBQL}/target/classes
jar cf ${RELEASE}/${DIST}.jar *

mkdir ${SITE}/downloads
cp -r ${SITE}/* ${RELEASE}/docs/

cd ${SITE}
zip -r ${SITE}/downloads/${DOCS}.zip apidocs

cd ${RELEASE}/..
zip -r ${SITE}/downloads/${DIST}.zip ${DIST}
tar cvf ${SITE}/downloads/${DIST}.tar ${DIST}
gzip ${SITE}/downloads/${DIST}.tar

cd ${HBQL}
mvn site-deploy
