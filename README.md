# BelegParallelAlgorithmsNeo4j

This is the Repo for my student research project  "Parallel Graphalgorithms in Neo4j"

This is work in progress, so things (mostly this readme/docs) tend do be outdated and will be changed.

# Quick Start

**1. Build it:**

This Java project is built with [Maven](http://maven.apache.org).
With this command maven will generate a jar file for you:

        mvn install

**2. Start it with**

        java -jar <JAR> A B C D E F G H I J
        
         A = Name of the Algorithmus (RW, SCC, WCC)
         B = OperationNumber (for RW) [for the other algorithms this parameter is irrelevant, but some value still needs to be present]
         C = Number of Runs
         D = Number of Threads
         E = WarmUp Time in S
         F = New SPI (true or false) (for RW) [for the other algorithms this parameter is irrelevant, but some value still needs to be present]
         G = PropertyName, under which the results will be saved at the nodes
         H = PageCache (String ala "6G")
         I = Path to DB
         J = Write or NoWrite
         
*Example:*
           
         java -jar <JAR> RW 1001000 10 8 120 true RandomWalkCounterTest 6G C:\\BelegDB\\data\\graph.db NoWrite
         
