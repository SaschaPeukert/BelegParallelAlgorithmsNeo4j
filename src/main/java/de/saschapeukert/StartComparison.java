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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    private  static final String DB_PATH = "E:\\Users\\Sascha\\Documents\\GIT\\_Belegarbeit\\neo4j-enterprise-2.3.0-M02\\data\\graph.db";

    private static final int OPERATIONS=10000;
    private static final int NUMBER_OF_THREADS =4;

    public static void main(String[] args)  {
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(DB_PATH)).newGraphDatabase();
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

        // Start single RandomWalker
        RandomWalkThread rwst = new RandomWalkThread(20,nodes, graphDb,OPERATIONS);
        Thread thr = new Thread(rwst);
        thr.start();
        try {
            thr.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("RandomWalker (SingleThread) done in " + rwst.timer.elapsed(TimeUnit.MICROSECONDS) + "\u00B5s (" + rwst.timer.elapsed(TimeUnit.MILLISECONDS) + "ms)");



        // 	comparison with NUMBER_OF_THREADS Threads
        //

        // Initialization of the Threads
        Map<Thread,RandomWalkThread> map = new HashMap<>();
        for(int i=0;i<NUMBER_OF_THREADS;i++){
            RandomWalkThread rw = new RandomWalkThread(20,nodes, graphDb,OPERATIONS/NUMBER_OF_THREADS);
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

        System.out.println("RandomWalker (MultiThread) done in " + elapsedTime + "\u00B5s (" +elapsedTime/1000 +"ms)");



                //System.out.println("Protocol:");
        //System.out.println(r.Protocol);
    }



    // START SNIPPET: shutdownHook
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
