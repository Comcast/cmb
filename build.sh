#!/bin/bash

# clean artifacts left behind by prior builds

mvn --settings ./settings.xml -f pom-cmb.xml clean

# build cmb-distribution-<version>.tar.gz for worker node, skipping tests

mvn --settings ./settings.xml -f pom-cmb.xml -Dmaven.test.skip=true assembly:assembly

# build cns-distribution-<version>.tar.gz containing cns.war and 
# cqs-distribution-<version>.tar.gz containing cqs.war for CMB service endpoints, 
# skipping tests

mvn --settings ./settings.xml -f pom-cns.xml -Dmaven.test.skip=true assembly:assembly
mvn --settings ./settings.xml -f pom-cqs.xml -Dmaven.test.skip=true assembly:assembly

# run all unit tests (unit tests only work after standing up a complete CMB environment)

# mvn test

# run individual unit tests

# mvn -Dtest=<TestName> test
