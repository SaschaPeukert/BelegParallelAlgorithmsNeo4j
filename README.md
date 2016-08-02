# BelegParallelAlgorithmsNeo4j

This is the Repository for my student research project  "Parallel Graphalgorithms in Neo4j" at TU Dresden. 
The final PDF document (in German) can be found [here](http://public.sascha-peukert.de/Beleg.pdf)

# Quick Start

**1. Build it:**

This Java project is built with [Maven](http://maven.apache.org).
With this command maven will generate a jar file for you:

        mvn install

**2. Use it**

You have two options to use the jar, you've just built. Use Neo4j als EmbeddedDatabase and run the jar by itself 
or have Neo4j run this jar as an unmanaged extension.

***Run as standalone***

Start it with

        java -jar <JAR> A B C D E F G H I 
        
         A = Name of the Algorithmus (RW, SCC, WCC, DegreeStats)
         B = OperationNumber (for RW) [for the other algorithms this parameter states eg. how many nodes (out of a list) a thread has to expand [BATCHSIZE]]
         C = Number of Runs
         D = Number of Threads * **
         E = Kernel API (true or false) (for RW) [for the other algorithms this parameter is irrelevant, but some value still needs to be present]
         F = PropertyName, under which the results will be saved at the nodes
         G = PageCache (String ala "6G")
         H = Path to DB
         I = Write or NoWrite
         
         
         * If D is greater than one, the corresponding multithread algorithm will automatically be used 
         ** Attention: If this Number is -1 and SCC with 1 Thread is executed, Multistep SCC with 1 Thread will be used. This little "hack" is intended.
         
*Examples:*
           
         java -jar <JAR> RW 1001000 10 8 true RandomWalkCounterTest 6G C:\\BelegDB\\data\\graph.db NoWrite
         java -jar <JAR> SCC 2000 10 8 true SCC_Nr 8G C:\\BelegDB\\data\\graph.db Write
         
***Run as unmanaged extension***

Please follow the steps from [Neo4j Unmanaged Extensions](http://neo4j.com/docs/stable/server-unmanaged-extensions.html) to get Neo4j to work with your jar.

Start the Algorithms with GET-Requests with the following syntax.
        
| Algorithm | Syntax of GET-Request                                                                         | 
|-----------|-----------------------------------------------------------------------------------------------|
| Warmup    | http://server:port/mountpoint/warmup  |
| | |
|           | _Example_: http://server:7474/extension/algorithms/warmup | 
| Random Walk | http://server:port/mountpoint/randomWalk |
| | /{PropertyName}/{Batchsize}/{Number_of_Threads} |
| | /{WriteBatchsize}/{Number_of_Steps}/{KernelAPI}  |
| | |
| | _Example_: http://server:7474/extension/algorithms/randomWalk |
| | /counter/1000/4 |
| |   /1000/100000/true |
| Weakly Connected Components |  http://server:port/mountpoint/weaklyConnectedComponents |
| | /PropertyName/Batchsize/Number_of_Threads/WriteBatchsize |
| | |
| | _Example_: http://server:7474/extension/algorithms/weaklyConnectedComponents |
| | /WeaklyComponentId/1000/4/1000 |     
| Strongly Connected Components |  http://server:port/mountpoint/stronglyConnectedComponents |
| | /PropertyName/Batchsize/Number_of_Threads/WriteBatchsize |
| | |
| | _Example_: http://server:7474/extension/algorithms/stronglyConnectedComponents |
| | /StronglyComponentId/1000/4/1000 |      
  
The parameter Number_of_Threads can also have the value "auto". This will automatically select a number of threads matching the number of cores in your cpu
