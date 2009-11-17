#! /bin/bash

export MAVEN_OPTS="-Xms256m -Xmx256m"

export HBQL="/Users/pambrose/git/hbase-plugin"
export VERSION="hbql-0.9.4-alpha"
export DOCS="javadocs-0.9.4-alpha"
export RELEASE=${HBQL}/target/release/${VERSION}
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
jar cf ${RELEASE}/${VERSION}.jar *

mkdir ${SITE}/downloads
cp -r ${SITE}/* ${RELEASE}/docs/

cd ${SITE}
zip -r ${SITE}/downloads/${DOCS}.zip apidocs

cd ${RELEASE}/..
zip -r ${SITE}/downloads/${VERSION}.zip ${VERSION}
tar cvf ${SITE}/downloads/${VERSION}.tar ${VERSION}
gzip ${SITE}/downloads/${VERSION}.tar

cd ${HBQL}
mvn site-deploy
