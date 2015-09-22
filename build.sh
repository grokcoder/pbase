#!/usr/bin/env bash

mvn clean package

cd exe
mkdir bin
mkdir conf
mkdir lib

cp hbase-*/target/hbase-*0.jar lib/
cp -r ../bin/* bin/
cp -r ../conf/* conf/
