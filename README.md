# BelegParallelAlgorithmsNeo4j

This is the Repo for my student research project  "Parallel Graphalgorithms in Neo4j"

This is work in progress, so things (mostly this readme/docs) tend do be outdated and will be changed.

# Quick Start

**1. Build it:**

This Java project is built with [Maven](http://maven.apache.org).
With this command maven will generate a jar file for you:

        mvn install

**2. Start it with**

        java -jar <JAR> A B C D E F G H I 
        
         A = Name of the Algorithmus (RW, SCC, WCC, DegreeStats)
         B = OperationNumber (for RW) [for the other algorithms this parameter is irrelevant**, but some value still needs to be present]
         C = Number of Runs
         D = Number of Threads *
         E = New SPI (true or false) (for RW) [for the other algorithms this parameter is irrelevant, but some value still needs to be present]
         F = PropertyName, under which the results will be saved at the nodes
         G = PageCache (String ala "6G")
         H = Path to DB
         I = Write or NoWrite
         
         
         * If D is greater than one, the corresponding multithread algorithm will automatically be used 
         ** Attention: If this Number is -1 and SCC with 1 Thread is executed, Multistep SCC with 1 Thread will be used. This little "hack" is intended.
         
*Example:*
           
         java -jar <JAR> RW 1001000 10 8 120 true RandomWalkCounterTest 6G C:\\BelegDB\\data\\graph.db NoWrite
         
