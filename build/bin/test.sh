#! /bin/bash

export MAVEN_OPTS="-Xms2048m -Xmx2048m"

mvn clean
mvn antlr3:antlr
mvn compiler:compile
mvn surefire:test
