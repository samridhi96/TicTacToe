#!/bin/sh
/home/NETID/sagra15/Storm/apache-maven-3.6.1/bin/mvn exec:java -Dexec.mainClass="TopologyMain" -Dexec.args="src/main/resources/flights.txt src/main/resources/airports.txt src/main/resources/AirlineCode.txt"
