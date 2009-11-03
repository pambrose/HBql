export MAVEN_OPTS="-Xms256m -Xmx256m"
mvn clean
mvn antlr3:antlr
mvn compiler:compile
mvn javadoc:javadoc
mvn site-deploy
