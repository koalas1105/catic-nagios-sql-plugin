#!/bin/sh
#--------------------------------------------------------------------------------
#
#--------------------------------------------------------------------------------
#echo "$*" >> /tmp/nagios_jsql.log
JAVA_HOME=/opt/sun/jdk1.7.0_21   # change
cd /usr/local/nagios/libexec/ics
JARS=commons-lang-2.3.jar:commons-cli-1.2.jar:ojdbc14.jar:dom4j-1.6.1.jar:log4j-1.2.16.jar:.
#${JAVA_HOME}/bin/javac -cp $JARS *.java
${JAVA_HOME}/bin/java -cp $JARS HiNagiosPlugin $* 
ret=$?
exit $ret
