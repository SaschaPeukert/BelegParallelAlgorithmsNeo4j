package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStore;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    //private static final String DB_PATH = "neo4j-enterprise-2.3.0-M02/data/graph.db";
    private  static final String DB_PATH = "C:\\BelegDB\\neo4j-enterprise-2.3.0-M02\\data\\graph.db";
    /*private  static final String DB_PATH = "E:\\Users\\Sascha\\Documents\\GIT\\" +
           "_Belegarbeit\\neo4j-enterprise-2.3.0-M02\\data\\graph.db";*/

    private static final int OPERATIONS=201000;
    private static final int NUMBER_OF_THREADS =8;
    private static final int NUMBER_OF_RUNS_TO_AVERAGE_RESULTS = 1; //Minimum: 1
    private static final int RANDOMWALKRANDOM = 20;  // Minimum: 1
    private static final int WARMUPTIME = 120; // in seconds
    private static final boolean NEWSPI = false;
    private static final String PROP_NAME = "RandomWalkCounter";
    private static int PROP_ID;

    private static Map<Long,Integer> results;

    public static void main(String[] args)  {

        System.out.println("Start: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

        System.out.println("I will start the RandomWalk Comparison: Single Thread vs. " + NUMBER_OF_THREADS +
               " Threads.\nEvery RandomWalk-Step (Count of Operations) is run " + NUMBER_OF_RUNS_TO_AVERAGE_RESULTS + " times.");

        // Open connection to DB
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH))
                .setConfig(GraphDatabaseSettings.pagecache_memory,"7G")
                .setConfig(GraphDatabaseSettings.allow_store_upgrade,"true")
                .newGraphDatabase();

        /*
        try(Transaction tx = graphDb.beginTx()){
            Result result = graphDb.execute("MATCH (n) WHERE n.RandomWalkCounter>0 RETURN id(n),n.RandomWalkCounter ORDER BY n.RandomWalkCounter DESC LIMIT 20");
            String rows="";
            while ( result.hasNext() )
            {
                Map<String,Object> row = result.next();
                for ( Map.Entry<String,Object> column : row.entrySet() )
                {
                    rows += column.getKey() + ": " + column.getValue() + "; ";
                }
                rows += "\n";
            }

            System.out.println(rows);
        }

        graphDb.shutdown();
        */

        /*
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
        int highestPropertyKey = DBUtils.getHighestPropertyID(graphDb);


        results = new HashMap<>(nodeIDhigh);

        System.out.println("~ " + nodeIDhigh + " Nodes");

        PROP_ID = ClearUp_AND_GetPropertyID(PROP_NAME, highestPropertyKey, graphDb);

        WarmUp(graphDb, nodeIDhigh, WARMUPTIME, true);

        System.out.println("");

        Stopwatch timeOfComparision = Stopwatch.createStarted();

        /*
        System.out.println(calculateConnectedComponentsComparison(graphDb,
                nodeIDhigh,NUMBER_OF_RUNS_TO_AVERAGE_RESULTS,
                ConnectedComponentsSingleThreadAlgorithm.AlgorithmType.STRONG));

        */


        System.out.println(
                calculateRandomWalkComparison(graphDb, nodeIDhigh, NUMBER_OF_RUNS_TO_AVERAGE_RESULTS)
        );


        timeOfComparision.stop();

        System.out.println("\nWhole Comparison done in: " + timeOfComparision.elapsed(TimeUnit.SECONDS) + "s (+ WarmUp " + WARMUPTIME + "s)");

        System.out.println("Writing the results ("+ results.keySet().size() +") to DB");
        writeResultsOut(graphDb);

        java.awt.Toolkit.getDefaultToolkit().beep();
        System.out.println("End: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

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
        ThreadLocalRandom random = ThreadLocalRandom.current();
        //try (Transaction tx = graphDb.beginTx()) {

        while (timer.elapsed(TimeUnit.SECONDS) < secs) {
                doMultiThreadRandomWalk(graphDb,highestNodeId,100);
                //if (random.nextBoolean()) {
                    //DBUtils.getSomeRandomNode(graphDb, random, highestNodeId);
                    //        } else {
                    //                  DBUtils.getSomeRandomRelationship(graphDb, random, highestNodeId);
//                }

            }

//            tx.success();
  //      }
        timer.stop();

        if(output)System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
        timer = null;
    }

    private static int ClearUp_AND_GetPropertyID(String propertyName, int highestPropertyKey, GraphDatabaseService graphDb){
        // TODO: Move?
        try(Transaction tx = graphDb.beginTx()) {

            ThreadToStatementContextBridge ctx = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            DataWriteOperations ops = ctx.get().dataWriteOperations();

            int lookup = ops.propertyKeyGetForName(propertyName);
            if(lookup==-1){
                System.out.println("Not found. Creating new ID");
                highestPropertyKey++;
                DBUtils.createNewPropertyKey(propertyName, highestPropertyKey, ops);

            } else{
                System.out.println("Found. Using old ID " + lookup);
                DBUtils.removePropertyFromAllNodes(lookup, ops, graphDb);
                tx.success();
                return lookup;
            }

            tx.success();

            return highestPropertyKey;  // can be used now, referencing now the correct PropertyKey.
            // No Node has a Property with this key jet

        } catch (InvalidTransactionTypeKernelException e) {
            e.printStackTrace();  // TODO REMOVE
        }

        return -1; // ERROR happend

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

            //System.gc();
        }

        resultsOfRun = resultsOfRun/runs;

        return "Result: " + resultsOfRun + "ms\n" + result;
    }

    private static long doConnectedComponentsRun(GraphDatabaseService graphDb,int highestNodeId, ConnectedComponentsSingleThreadAlgorithm.AlgorithmType type){

        ConnectedComponentsSingleThreadAlgorithm ConnectedSingle = new ConnectedComponentsSingleThreadAlgorithm(
                graphDb, highestNodeId, PROP_ID,PROP_NAME, type);
        Thread t = new Thread(ConnectedSingle);

        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(ConnectedSingle.getResults());

        return ConnectedSingle.timer.elapsed(TimeUnit.MILLISECONDS);

    }

    private static String calculateRandomWalkComparison(GraphDatabaseService graphDb, int highestNodeId,
                                                        int numberOfRunsPerStep){
        String result="\n\nSteps SingleThread[\u00B5s] MultiThread[\u00B5s] SpeedUp[%]";

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

            result += "\n" +c + " " + resultsOfStep[0] + " " + resultsOfStep[1] + " " +
                    (100-(((float)100/resultsOfStep[0])*resultsOfStep[1]));

            resultsOfStep = null; // suggestion for garbage collector

           //System.gc(); // suggestion for garbage collector. Now would be perfect!

        }

        return result;
    }


    private static long[] doRandomWalkerRun(GraphDatabaseService graphDb,int highestNodeId, int noOfSteps){
        long[] runtimes = new long[2];

        runtimes[0] = doSingleThreadRandomWalk(graphDb, highestNodeId, noOfSteps);

        // 	comparison with NUMBER_OF_THREADS Threads
        //

        runtimes[1] =doMultiThreadRandomWalk(graphDb,highestNodeId,noOfSteps);

        return runtimes;

    }

    private static long doMultiThreadRandomWalk(GraphDatabaseService graphDb, int highestNodeId, int noOfSteps){

        // INIT
        Map<Thread,AlgorithmRunnable> map = new HashMap<>();
        for(int i=0;i<NUMBER_OF_THREADS;i++){

            AlgorithmRunnable rw;
            if(NEWSPI){
                rw = new RandomWalkAlgorithmRunnableNewSPI(RANDOMWALKRANDOM,
                        graphDb,highestNodeId,PROP_ID,PROP_NAME, noOfSteps/NUMBER_OF_THREADS);
            } else{
                rw = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM,
                        graphDb,highestNodeId,PROP_ID,PROP_NAME, noOfSteps/NUMBER_OF_THREADS);
            }

            map.put(rw.getNewThread(),rw);
        }

        // Thread start
        //map.keySet().forEach(java.lang.Thread::start);

        for(Thread t:map.keySet()){
            t.start();
        }


        // Thread join
        long elapsedTime=0;
        for(Thread t:map.keySet()){
            try {
                t.join();

                if(elapsedTime<map.get(t).timer.elapsed(TimeUnit.MICROSECONDS)){
                    elapsedTime =map.get(t).timer.elapsed(TimeUnit.MICROSECONDS);
                }

                addToResults(map.get(t).result);

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

        AlgorithmRunnable rwst;
        if(NEWSPI){
            rwst = new RandomWalkAlgorithmRunnableNewSPI(RANDOMWALKRANDOM, graphDb,highestNodeId,PROP_ID,PROP_NAME,noOfSteps);
        } else{
            rwst = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM, graphDb,highestNodeId,PROP_ID,PROP_NAME,noOfSteps);

        }
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


        addToResults(rwst.result);

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

    public static void addToResults(Map<Long,Integer> map){

        for(Long l:map.keySet()){

            Integer null_or_oldValue = results.get(l);
            if(null_or_oldValue==null){
                results.put(l,map.get(l));
            } else{
                results.put(l,null_or_oldValue+map.get(l));
            }
        }

    }

    public static boolean writeResultsOut(GraphDatabaseService graphDb){

        try(Transaction tx = graphDb.beginTx()) {

            ThreadToStatementContextBridge ctx =((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            DataWriteOperations ops = ctx.get().dataWriteOperations();

            for (Long l : results.keySet()) {
                DBUtils.createIntPropertyAtNode(l, results.get(l), PROP_ID, ops);
            }

            tx.success();

        } catch (InvalidTransactionTypeKernelException e) {
            e.printStackTrace(); // TODO remove
            return false;
        }

        return true;
    }
}
