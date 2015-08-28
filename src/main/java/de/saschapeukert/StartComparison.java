package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.NeoStore;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    private  static final String DB_PATH = "E:\\Users\\Sascha\\Documents\\GIT\\" +
            "_Belegarbeit\\neo4j-enterprise-2.3.0-M02\\data\\graph.db";

    private static final int OPERATIONS=200000;
    private static final int NUMBER_OF_THREADS =4;
    private static final int NUMBER_OF_RUNS_TO_AVERAGE_RESULTS = 1; //Minimum: 1
    private static final int RANDOMWALKRANDOM = 20;  // Minimum: 1
    private static final int WARMUPTIME = 60; // in seconds

    public static void main(String[] args)  {

        System.out.println("I will start the RandomWalk Comparison: Single Thread vs. " + NUMBER_OF_THREADS +
                " Threads.\nEvery RandomWalk-Step (Count of Operations) is run " + NUMBER_OF_RUNS_TO_AVERAGE_RESULTS + " times.");


        // Open connection to DB
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH))
                .setConfig(GraphDatabaseSettings.pagecache_memory,"4G")
             //   .setConfig(GraphDatabaseSettings.allow_store_upgrade,"true")
                .newGraphDatabase();

        /*GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder(DB_PATH);

        builder.setConfig(ClusterSettings.server_id, "1");
        builder.setConfig(HaSettings.ha_server, "localhost:6001");
        builder.setConfig(HaSettings.slave_only, Settings.FALSE);
        builder.setConfig(ClusterSettings.cluster_server, "localhost:5001");
        builder.setConfig(ClusterSettings.initial_hosts, "localhost:5001,localhost:5002");

        GraphDatabaseService graphDb = builder.newGraphDatabase();
        */
        registerShutdownHook(graphDb);


        int nodeIDhigh = getHighestNodeID(graphDb);

        System.out.println("~ " +nodeIDhigh + " Nodes");

        WarmUp(graphDb, nodeIDhigh, WARMUPTIME, true);

        System.out.println("");

        Stopwatch timeOfComparision = Stopwatch.createStarted();

        System.out.println(
                calculateRandomWalkComparison(graphDb, nodeIDhigh, NUMBER_OF_RUNS_TO_AVERAGE_RESULTS)
        );



        /*AlgorithmRunnable rwSPI = new RandomWalkAlgorithmRunnableNewSPI(RANDOMWALKRANDOM,nodeIDhigh,
                graphDb,NUMBER_OF_RUNS_TO_AVERAGE_RESULTS);
        Thread thread = new Thread(rwSPI);*/



        timeOfComparision.stop();

        System.out.println("\nWhole Comparison done in: "+ timeOfComparision.elapsed(TimeUnit.SECONDS)+"s (+ WarmUp)");

        //System.out.println(calculateConnectedComponentsComparison(graphDb,
        //        nodes,NUMBER_OF_RUNS_TO_AVERAGE_RESULTS,
        //        ConnectedComponentsSingleThreadAlgorithm.AlgorithmType.STRONG));


    }

    /**
     * WarmUp
     * @param graphDb
     * @param highestNodeId
     * @param secs
     */
    private static void WarmUp(GraphDatabaseService graphDb,int highestNodeId, int secs, boolean output){
        if(output) System.out.println("Starting WarmUp.");

        Stopwatch timer = Stopwatch.createStarted();
        while(timer.elapsed(TimeUnit.SECONDS)<secs){
            doMultiThreadRandomWalk(graphDb,highestNodeId,10000);
        }
        timer.stop();

        if(output)System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
        timer = null;
    }


    private static String calculateConnectedComponentsComparison
            (GraphDatabaseService graphDb, int highestNodeId, int runs,
             ConnectedComponentsSingleThreadAlgorithm.AlgorithmType type){

        long resultsOfRun =0;
        String result = "";
        for(int i=0;i<runs;i++){
            long temp = doConnectedComponentsRun(graphDb,highestNodeId,type);
            resultsOfRun = resultsOfRun + temp;
            result += "("+i+") " + temp + "ms\n";

            System.gc();
        }

        resultsOfRun = resultsOfRun/runs;

        return "Result: " + resultsOfRun + "ms\n" + result;
    }

    private static long doConnectedComponentsRun(GraphDatabaseService graphDb,int highestNodeId, ConnectedComponentsSingleThreadAlgorithm.AlgorithmType type){

        ConnectedComponentsSingleThreadAlgorithm ConnectedSingle = new ConnectedComponentsSingleThreadAlgorithm(
                graphDb, highestNodeId, type);
        Thread t = new Thread(ConnectedSingle);

        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return ConnectedSingle.timer.elapsed(TimeUnit.MILLISECONDS);

    }

    private static String calculateRandomWalkComparison(GraphDatabaseService graphDb, int highestNodeId,
                                                        int numberOfRunsPerStep){
        String result="";

        long[] resultsOfStep;
        long[] resultsOfRun;

        for(int c=1000;c<=OPERATIONS;c=c+5000){
            // Step
            System.out.println("Now doing step " +c + "/" + OPERATIONS);
            resultsOfStep = new long[2];

            for(int i=0;i<numberOfRunsPerStep;i++){
                // Run
                resultsOfRun = doRandomWalkerRun(graphDb, highestNodeId,c);

                resultsOfStep[0] += resultsOfRun[0];
                resultsOfStep[1] += resultsOfRun[1];

                resultsOfRun = null; // suggestion for garbage collector

            }
            // calculating average of Runs
            resultsOfStep[0] = resultsOfStep[0]/numberOfRunsPerStep;
            resultsOfStep[1] = resultsOfStep[1]/numberOfRunsPerStep;

            result += "\n" +c + " " + resultsOfStep[0] + " " + resultsOfStep[1];

            resultsOfStep = null; // suggestion for garbage collector

            System.gc(); // suggestion for garbage collector. Now would be perfect!

        }

        return result;
    }


    private static long[] doRandomWalkerRun(GraphDatabaseService graphDb,int highestNodeId, int noOfSteps){
        long[] runtimes = new long[2];

        runtimes[0] = doSingleThreadRandomWalk(graphDb, highestNodeId, noOfSteps);

        //WarmUp(graphDb,highestNodeId,5,false);

        // 	comparison with NUMBER_OF_THREADS Threads
        //

        runtimes[1] =doMultiThreadRandomWalk(graphDb,highestNodeId,noOfSteps);

        return runtimes;

    }

    private static long doMultiThreadRandomWalk(GraphDatabaseService graphDb, int highestNodeId, int noOfSteps){

        // INIT
        Map<Thread,AlgorithmRunnable> map = new HashMap<>();
        for(int i=0;i<NUMBER_OF_THREADS;i++){
            RandomWalkAlgorithmRunnable rw = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM,
                    graphDb,highestNodeId, noOfSteps/NUMBER_OF_THREADS);
            map.put(rw.getNewThread(),rw);
        }

        // Thread start
        map.keySet().forEach(java.lang.Thread::start);

        // Thread join
        long elapsedTime=0;
        for(Thread t:map.keySet()){
            try {
                t.join();

                if(elapsedTime<map.get(t).timer.elapsed(TimeUnit.MICROSECONDS)){
                    elapsedTime =map.get(t).timer.elapsed(TimeUnit.MICROSECONDS);
                }
                t = null; // suggestion for garbage collector

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        map = null; // suggestion for garbage collector


        /*System.out.println("RandomWalk (MultiThread "+ noOfSteps + " steps) done in " + elapsedTime +
                "\u00B5s (" +elapsedTime/1000 +"ms)");  */
        return elapsedTime;
    }

    private static long doSingleThreadRandomWalk(GraphDatabaseService graphDb, int highestNodeId, int noOfSteps){
        AlgorithmRunnable rwst = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM, graphDb,highestNodeId,noOfSteps);
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
        thr = null;

        return rwst.timer.elapsed(TimeUnit.MICROSECONDS);

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


    public static int getHighestNodeID(GraphDatabaseService graphDb ){

        NeoStore neoStore = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(NeoStore.class);
        return (int) neoStore.getNodeStore().getHighId();

    }
}
