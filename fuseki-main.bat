@echo off
@rem Licensed under the terms of http://www.apache.org/licenses/LICENSE-2.0

java -cp jena-fuseki-server-4.5.0.jar:lib/* org.apache.jena.fuseki.main.cmds.FusekiMainCmd %*
exit /B
