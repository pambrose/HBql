#! /bin/bash

cp -r ./lib ./target/hbql-0.9.0-alpha
cp -r ./bin ./target/hbql-0.9.0-alpha

mkdir ./target/hbql-0.9.0-alpha/src
cp -r ./src/main ./target/hbql-0.9.0-alpha/src
cp -r ./src/test ./target/hbql-0.9.0-alpha/src
rm -rf ./target/hbql-0.9.0-alpha/src/main/antlr3/org/apache/hadoop/hbase/contrib/hbql/antlr/output

cp LICENSE.txt ./target/hbql-0.9.0-alpha
cp NOTICE.txt ./target/hbql-0.9.0-alpha

mkdir ./target/hbql-0.9.0-alpha/docs
cp -r ./target/site/* ./target/hbql-0.9.0-alpha/docs/

cd target/classes
jar cf ../../target/hbql-0.9.0-alpha/hbql-0.9.0-alpha.jar *
cd ../..

mkdir ./target/site/downloads
cd target
zip -r ./site/downloads/hbql-0.9.0-alpha.zip hbql-0.9.0-alpha
cd ..

cd ./target/site
zip -r ../../target/site/downloads/javadocs-0.9.0-alpha.zip apidocs
cd ../..

mvn site-deploy
