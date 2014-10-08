#!/bin/sh

SCRIPT_DIR="$(dirname "$0")"
JAVA_OPTS=" -Xmx256m"
java $JAVA_OPTS -classpath "$SCRIPT_DIR/../conf:$SCRIPT_DIR/../lib/*" -Dlogback.configurationFile=$SCRIPT_DIR/../conf/logback.xml dk.statsbiblioteket.medieplatform.bitrepository.purger.PurgerCli
