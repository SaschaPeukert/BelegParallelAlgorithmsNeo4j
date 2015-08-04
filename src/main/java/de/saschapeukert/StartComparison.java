package de.saschapeukert;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    private  static final String DB_PATH = "E:\\Users\\Sascha\\Documents\\GIT\\" +
            "_Belegarbeit\\neo4j-enterprise-2.3.0-M02\\data\\graph.db";

    private static final int OPERATIONS=10000;
    private static final int NUMBER_OF_THREADS =4;

    public static void main(String[] args)  {

        // Open connection to DB
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH)).newGraphDatabase();
        registerShutdownHook(graphDb);

        // Initialise allNodes
        ArrayList<Node> nodes = new ArrayList<>();

        try (Transaction tx = graphDb.beginTx()) {
            GlobalGraphOperations operations = GlobalGraphOperations.at(graphDb);
            ResourceIterable<Node> it = operations.getAllNodes();

            for (Node n : it) {
                nodes.add(n);

            }

            tx.success();
        }
        /*
        String diagramm="";
        for(int c=500;c<=OPERATIONS;c=c+500){
            long[] results = doRandomWalkerRun(graphDb,nodes, c);
            diagramm += "\n" +c + ", " + results[0] + ", " + results[1];
        }

        System.out.println(diagramm);
        */

        doRandomWalkerRun(graphDb,nodes,OPERATIONS);
    }


    private static long[] doRandomWalkerRun(GraphDatabaseService graphDb, List<Node> nodes, int noOfSteps){
        long[] runtimes = new long[2];

        // Start single RandomWalker
        RandomWalkThread rwst = new RandomWalkThread(20,nodes, graphDb,noOfSteps);
        Thread thr = new Thread(rwst);
        thr.start();
        try {
            thr.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("RandomWalk (SingleThread " + noOfSteps + " steps) done in " + rwst.timer.elapsed(TimeUnit.MICROSECONDS) +
                "\u00B5s (" + rwst.timer.elapsed(TimeUnit.MILLISECONDS) + "ms)");
        runtimes[0] = rwst.timer.elapsed(TimeUnit.MICROSECONDS);


        // 	comparison with NUMBER_OF_THREADS Threads
        //

        // Initialization of the Threads
        Map<Thread,RandomWalkThread> map = new HashMap<>();
        for(int i=0;i<NUMBER_OF_THREADS;i++){
            RandomWalkThread rw = new RandomWalkThread(20,nodes, graphDb,noOfSteps/NUMBER_OF_THREADS);
            Thread t = new Thread(rw);
            map.put(t,rw);
        }

        // Thread start
        map.keySet().forEach(java.lang.Thread::start);

        long elapsedTime=0;
        for(Thread t:map.keySet()){
            try {
                t.join();
                elapsedTime +=map.get(t).timer.elapsed(TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("RandomWalk (MultiThread "+ noOfSteps + " steps) done in " + elapsedTime + "\u00B5s (" +elapsedTime/1000 +"ms)");
        runtimes[1] =elapsedTime;

        return runtimes;

    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
