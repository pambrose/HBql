#! /bin/bash

export MAVEN_OPTS="-Xms256m -Xmx256m"
mvn clean

mkdir -p ./target/hbql-0.9.3-alpha/

mvn antlr3:antlr
mvn compiler:compile
mvn javadoc:javadoc
mvn site:site

