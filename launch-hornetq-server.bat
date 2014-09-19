@ echo off
setlocal ENABLEDELAYEDEXPANSION
REM This batch file is adapted from run.bat from /bin in HornetQ.
REM [TODO] Set your HornetQ home folder below
SET HORNETQ_HOME=D:\Programming_software\hornetq-2.4.0.Final
SET MY_CONFIG_DIR=.\server
SET CONFIG_DIR=%HORNETQ_HOME%\config\stand-alone\non-clustered
REM Server config files are loaded as long as they are in classpath
set CLASSPATH=%CONFIG_DIR%;%HORNETQ_HOME%\schema\;%MY_CONFIG_DIR%
set JVM_ARGS=%CLUSTER_PROPS% -XX:+UseParallelGC  -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Dhornetq.config.dir=%CONFIG_DIR% -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Djava.util.logging.config.file=%CONFIG_DIR%\logging.properties -Djava.library.path=.
for /R %HORNETQ_HOME%\lib %%A in (*.jar) do (
SET CLASSPATH=!CLASSPATH!;%%A
)
echo ***********************************************************************************
echo "java %JVM_ARGS% -classpath %CLASSPATH% org.hornetq.integration.bootstrap.HornetQBootstrapServer hornetq-beans.xml"
echo ***********************************************************************************
java %JVM_ARGS% -classpath "%CLASSPATH%" org.hornetq.integration.bootstrap.HornetQBootstrapServer hornetq-beans.xml
