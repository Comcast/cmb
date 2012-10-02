#!/bin/bash

# clean artifacts left behind by prior builds

mvn --settings ./settings.xml clean

# build cmb.jar for worker node, skipping tests

mvn --settings ./settings.xml -Dprojectname=CMB -Dmaven.test.skip=true compile jar:jar

# build CNS.war and CQS.war for CMB service endpoints, skipping tests

mvn --settings ./settings.xml -Dprojectname=CNS -Dmaven.test.skip=true assembly:assembly
mvn --settings ./settings.xml -Dprojectname=CQS -Dmaven.test.skip=true assembly:assembly

# run all unit tests (unit tests only work after standing up a complete CMB environment)

# mvn test