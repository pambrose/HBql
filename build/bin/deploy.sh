#! /bin/bash

cp -r ./lib ./target/hbql-*
cp -r ./bin ./target/hbql-*

mkdir ./target/hbql-0.9.3-alpha/src
cp -r ./src/main ./target/hbql-*/src
cp -r ./src/test ./target/hbql-*/src
rm -rf ./target/hbql-*/src/main/antlr3/org/apache/hadoop/hbase/hbql/antlr/output

cp LICENSE.txt ./target/hbql-*
cp NOTICE.txt ./target/hbql-*

mkdir ./target/hbql-0.9.3-alpha/docs
cp -r ./target/site/* ./target/hbql-*/docs/

cd target/classes
jar cf ../../target/hbql-0.9.3-alpha/hbql-0.9.3-alpha.jar *
cd ../..

mkdir ./target/site/downloads
cd target
zip -r ./site/downloads/hbql-0.9.3-alpha.zip hbql-0.9.3-alpha
cd ..

cd ./target/site
zip -r ../../target/site/downloads/javadocs-0.9.3-alpha.zip apidocs
cd ../..

mvn site-deploy
