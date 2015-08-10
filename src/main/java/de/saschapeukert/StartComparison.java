package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
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
    private static final int NUMBER_OF_RUNS_TO_AVERAGE_RESULTS = 1; //Minimum: 1

    public static void main(String[] args)  {

        /*
        System.out.println("I will start the RandomWalk Comparison: Single Thread vs. " + NUMBER_OF_THREADS +
                " Threads.\nEvery RandomWalk-Step (Count of Operations) is run " + NUMBER_OF_RUNS_TO_AVERAGE_RESULTS + " times.");
        */

        Stopwatch timeOfComparision = Stopwatch.createStarted();


        // Open connection to DB
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH)).newGraphDatabase();
        registerShutdownHook(graphDb);

        /*      ??? depricated + non-existent docs
        GraphDatabaseAPI api = ((GraphDatabaseAPI) graphDb);
        ThreadToStatementContextBridge ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        ReadOperations ops =  ctx.instance().readOperations();

        */



        /*
        List<Node> nodes = getAllNodes(graphDb);

        System.out.println(
                calculateResults(graphDb,nodes,NUMBER_OF_RUNS_TO_AVERAGE_RESULTS)
        );
        timeOfComparision.stop();

        System.out.println("\nWhole Comparison done in: "+ timeOfComparision.elapsed(TimeUnit.SECONDS)+"s");
        */

        ConnectedComponentsSingleThreadAlgorithm ConnectedSingle = new ConnectedComponentsSingleThreadAlgorithm(
                graphDb, ConnectedComponentsSingleThreadAlgorithm.AlgorithmType.STRONG);
        Thread t = new Thread(ConnectedSingle);

        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(ConnectedSingle.getResults());

    }

    private static String calculateResults(GraphDatabaseService graphDb, List<Node> nodes,
                                           int NumberOfRunsPerStep){
        String result="";

        long[] resultsOfStep;
        long[] resultsOfRun;

        for(int c=100;c<=OPERATIONS;c=c*2){
            // Step
            resultsOfStep = new long[2];

            for(int i=0;i<NumberOfRunsPerStep;i++){
                // Run
                resultsOfRun = doRandomWalkerRun(graphDb,nodes, c);

                resultsOfStep[0] += resultsOfRun[0];
                resultsOfStep[1] += resultsOfRun[1];

                resultsOfRun = null; // suggestion for garbage collector

            }
            // calculating average of Runs
            resultsOfStep[0] = resultsOfStep[0]/NumberOfRunsPerStep;
            resultsOfStep[1] = resultsOfStep[1]/NumberOfRunsPerStep;

            result += "\n" +c + " " + resultsOfStep[0] + " " + resultsOfStep[1];

            resultsOfStep = null; // suggestion for garbage collector

            System.gc(); // suggestion for garbage collector. Now would be perfect!

        }

        return result;
    }


    private static long[] doRandomWalkerRun(GraphDatabaseService graphDb, List<Node> nodes, int noOfSteps){
        long[] runtimes = new long[2];

        // Start single RandomWalker
        AlgorithmRunnable rwst = new RandomWalkAlgorithmRunnable(20,nodes, graphDb,noOfSteps);
        Thread thr = rwst.getNewThread();
        thr.start();
        try {
            thr.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /*System.out.println("RandomWalk (SingleThread " + noOfSteps + " steps) done in " +
                rwst.timer.elapsed(TimeUnit.MICROSECONDS) +
                "\u00B5s (" + rwst.timer.elapsed(TimeUnit.MILLISECONDS) + "ms)"); */
        runtimes[0] = rwst.timer.elapsed(TimeUnit.MICROSECONDS);

        thr = null; // suggestion for garbage collector


        // 	comparison with NUMBER_OF_THREADS Threads
        //

        // Initialization of the Threads
        Map<Thread,AlgorithmRunnable> map = new HashMap<>();
        for(int i=0;i<NUMBER_OF_THREADS;i++){
            RandomWalkAlgorithmRunnable rw = new RandomWalkAlgorithmRunnable(20,nodes,
                    graphDb,noOfSteps/NUMBER_OF_THREADS);
            map.put(rw.getNewThread(),rw);
        }

        // Thread start
        map.keySet().forEach(java.lang.Thread::start);

        long elapsedTime=0;
        for(Thread t:map.keySet()){
            try {
                t.join();
                elapsedTime +=map.get(t).timer.elapsed(TimeUnit.MICROSECONDS);
                t = null; // suggestion for garbage collector

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        map = null; // suggestion for garbage collector


        /*System.out.println("RandomWalk (MultiThread "+ noOfSteps + " steps) done in " + elapsedTime +
                "\u00B5s (" +elapsedTime/1000 +"ms)");  */
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


    public static List<Node> getAllNodes(GraphDatabaseService graphDb ){
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
        return nodes;
    }
}
