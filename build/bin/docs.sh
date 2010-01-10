#!/bin/bash

export MAVEN_OPTS="-Xms2048m -Xmx2048m"

mvn -DskipTests=true site