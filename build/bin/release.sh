#! /bin/bash

export MAVEN_OPTS="-Xms256m -Xmx256m"

mvn clean

mkdir -p ./target/hbql-0.9.4-alpha/

mvn antlr3:antlr
mvn compiler:compile
mvn javadoc:javadoc
mvn site:site

cp -r ./lib ./target/hbql-*
cp -r ./bin ./target/hbql-*

mkdir ./target/hbql-0.9.4-alpha/src
cp -r ./src/main ./target/hbql-*/src
cp -r ./src/test ./target/hbql-*/src
rm -rf ./target/hbql-*/src/main/antlr3/org/apache/hadoop/hbase/hbql/antlr/output

cp LICENSE.txt ./target/hbql-*
cp NOTICE.txt ./target/hbql-*

mkdir ./target/hbql-0.9.4-alpha/docs
cp -r ./target/site/* ./target/hbql-*/docs/

cd target/classes
jar cf ../../target/hbql-0.9.4-alpha/hbql-0.9.4-alpha.jar *
cd ../..

mkdir ./target/site/downloads
cd target
zip -r ./site/downloads/hbql-0.9.4-alpha.zip hbql-0.9.4-alpha
cd ..

cd ./target/site
zip -r ../../target/site/downloads/javadocs-0.9.4-alpha.zip apidocs
cd ../..

mvn site-deploy
