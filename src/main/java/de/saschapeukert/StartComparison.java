package de.saschapeukert;

import com.google.common.base.Stopwatch;
import de.saschapeukert.Algorithms.Impl.ConnectedComponentsSingleThreadAlgorithm;
import de.saschapeukert.Algorithms.Impl.RandomWalkAlgorithmRunnable;
import de.saschapeukert.Algorithms.Impl.RandomWalkAlgorithmRunnableNewSPI;
import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Database.NeoWriter;
import org.HdrHistogram.Histogram;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    private  static String DB_PATH;
    // Server: neo4j-enterprise-2.3.0-M02/data/graph.db
    // SSD: C:\\BelegDB\\neo4j-enterprise-2.3.0-M02\\data\\graph.db
    // Meine HDD: E:\\Users\\Sascha\\Documents\\GIT\\Belegarbeit\\neo4j-enterprise-2.3.0-M02\\data\\graph.db

    private static int OPERATIONS;
    private static int NUMBER_OF_THREADS;
    private static int NUMBER_OF_RUNS; // = 30; //Minimum: 1
    private static final int RANDOMWALKRANDOM = 20;  // Minimum: 1
    private static  int WARMUPTIME; // in seconds
    private static  boolean NEWSPI;
    private static  String PROP_NAME;
    private static int PROP_ID;
    private static String PAGECACHE;
    private static String ALGORITHM;
    private static String WRITE;

    public static Map<Long,AtomicInteger> resultCounter;
    public static Object[] keySetOfResultCounter;  // TODO: REFACTOR THE VISIBILTY?

    private static Histogram histogram = new Histogram(3600000000000L, 3);


    public static void main(String[] args)  {

        // READING THE INPUTPARAMETER
        try {
            ALGORITHM = args[0];
            OPERATIONS = Integer.valueOf(args[1]); // AS LONG AS ITS RW
                                                    // TODO: NEED START

            NUMBER_OF_RUNS = Integer.valueOf(args[2]);
            NUMBER_OF_THREADS = Integer.valueOf(args[3]);
            WARMUPTIME = Integer.valueOf(args[4]);
            if (args[5].equals("true")) {
                NEWSPI = true;
            } else {
                NEWSPI = false;
            }
            PROP_NAME = args[6];
            PAGECACHE= args[7];
            DB_PATH = args[8];
            WRITE = args[9];

        } catch(Exception e){

            System.out.println("Not enough input parameter.");
            System.out.println("You have to supply: AlgorithmName OperationNumber Number_of_Threads WarmUpTime_in_s " +
                    "NewSPI_bool PropertyName PageCache(String)_in_G/M/K DB-Path");
            return;
        }

        // START
        System.out.println("Start: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

        System.out.println("I will start the "+ ALGORITHM +" with: " + NUMBER_OF_THREADS +
               " Thread(s).\n");


        // Open connection to DB
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH))
                .setConfig(GraphDatabaseSettings.pagecache_memory, PAGECACHE)
                .setConfig(GraphDatabaseSettings.keep_logical_logs,"false")  // to get rid of all those neostore.trasaction.db ... files
                .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
              //  .setConfig(GraphDatabaseSettings.read_only,"true")
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


        int nodeIDhigh = DBUtils.getHighestNodeID(graphDb);
        int highestPropertyKey = DBUtils.getHighestPropertyID(graphDb);


        Transaction t = DBUtils.openTransaction(graphDb);
            prepaireResultMapAndCounter(graphDb, nodeIDhigh);
        DBUtils.closeTransactionSuccess(t);

        System.out.println("~ " + nodeIDhigh + " Nodes");


        if(WRITE.equals("Write")){

            PROP_ID = DBUtils.GetPropertyID(PROP_NAME, highestPropertyKey, graphDb);

            if(PROP_ID==-1){
                // ERROR Handling
                System.out.println("Something went wrong while looking up or creating the PropertyID. See Stacktrace for Answers.");
                return;
            }
        }

        WarmUp(graphDb, nodeIDhigh, WARMUPTIME, true,false);

        System.out.println("");

        Stopwatch timeOfComparision = Stopwatch.createStarted();


        switch (ALGORITHM){
            case "RW_old":
                System.out.println("Overwrite: Only doing 1 Run per Step");
                System.out.println(
                        calculateRandomWalk_old(graphDb, nodeIDhigh, 1, true) // REMOVED MORE THEN ONE RUN! Because...
                );
                break;
            case "RW_new":
                calculateRandomWalk_new(graphDb, nodeIDhigh, NUMBER_OF_RUNS, true);
                break;
            case "WCC":
                calculateConnectedComponents(graphDb,
                        nodeIDhigh, NUMBER_OF_RUNS,
                        ConnectedComponentsSingleThreadAlgorithm.AlgorithmType.WEAK, true);
                break;
            case "SCC":
                calculateConnectedComponents(graphDb,
                        nodeIDhigh, NUMBER_OF_RUNS,
                        ConnectedComponentsSingleThreadAlgorithm.AlgorithmType.STRONG, true);
                break;
            default:

                System.out.println("Error: Unknown Algorithm.");
                return;
        }


        timeOfComparision.stop();

        System.out.println("\nWhole Comparison done in: " + timeOfComparision.elapsed(TimeUnit.SECONDS) + "s (+ WarmUp " + WARMUPTIME + "s)");

        if(WRITE.equals("Write"))
            writeResultsOut(graphDb, nodeIDhigh);

        //java.awt.Toolkit.getDefaultToolkit().beep();
        System.out.println("End: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));


    }

    /**
     * WarmUp
     * @param graphDb
     * @param highestNodeId
     * @param secs
     */
    private static void WarmUp(GraphDatabaseService graphDb,int highestNodeId, int secs, boolean StringOutput, boolean DbwriteOutput){
        if(StringOutput) System.out.println("Starting WarmUp.");

        Stopwatch timer = Stopwatch.createStarted();

        while (timer.elapsed(TimeUnit.SECONDS) < secs) {
                doMultiThreadRandomWalk(graphDb,highestNodeId,100,DbwriteOutput);

            }

        timer.stop();

        if(StringOutput)System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
        timer = null;
    }

    private static void calculateConnectedComponents
            (GraphDatabaseService graphDb, int highestNodeId, int runs,
             ConnectedComponentsSingleThreadAlgorithm.AlgorithmType type, boolean output){

        for(int i=0;i<runs;i++){
            System.out.println("Now doing run " + (i + 1));

            histogram.recordValue(doConnectedComponentsRun(graphDb, highestNodeId, type, output));

        }

        System.out.println("");
        System.out.println("Times of CC with " + NUMBER_OF_THREADS + " Threads:");

        histogram.outputPercentileDistribution(System.out, 1.00);
    }


    private static String calculateRandomWalk_old(GraphDatabaseService graphDb, int highestNodeId,
                                                  int numberOfRunsPerStep, boolean output){
        String result="\n\nSteps SingleThread[\u00B5s] MultiThread[\u00B5s] SpeedUp[%]";

        long[] resultsOfStep;
        long[] resultsOfRun;

        for(int c=1000;c<=OPERATIONS;c=c+5000){
            // Step
            System.out.println("Now doing step " +c + "/" + OPERATIONS);
            resultsOfStep = new long[2];

            for(int i=0;i<numberOfRunsPerStep;i++){
                // Run
                resultsOfRun = doRandomWalkerRun(graphDb, highestNodeId,c,output);

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

    private static void calculateRandomWalk_new(GraphDatabaseService graphDb, int highestNodeId,
                                                int numberOfRunsPerStep, boolean output){

        for(int i=0;i<numberOfRunsPerStep;i++){
            System.out.println("Now doing run " + (i + 1));
            // Run
            long run = doMultiThreadRandomWalk(graphDb,highestNodeId,OPERATIONS, output);
            //System.out.println(run);
            histogram.recordValue(run);

        }

        System.out.println("");
        System.out.println("Times of RW with " + NUMBER_OF_THREADS +" Threads:");

        histogram.outputPercentileDistribution(System.out, 1.00);

    }


    /**
     *
     * @param graphDb
     * @param highestNodeId
     * @param type
     * @param output
     * @return elapsed time as MILLISECONDS!
     */

    private static long doConnectedComponentsRun(GraphDatabaseService graphDb,int highestNodeId,
                                                 ConnectedComponentsSingleThreadAlgorithm.AlgorithmType type, boolean output){

        ConnectedComponentsSingleThreadAlgorithm ConnectedSingle = new ConnectedComponentsSingleThreadAlgorithm(
                graphDb, highestNodeId, type, output);
        Thread t = new Thread(ConnectedSingle);
        t.setName("ConnectedComponentsSingleThreadAlgo");

        t.start();
        try {
            t.join();
            t = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //System.out.println(ConnectedSingle.getResults());    //TODO REMOVE, JUST FOR DEBUG

        return ConnectedSingle.timer.elapsed(TimeUnit.MILLISECONDS);

    }

    private static long[] doRandomWalkerRun(GraphDatabaseService graphDb,int highestNodeId, int noOfSteps, boolean output){
        long[] runtimes = new long[2];

        runtimes[0] = doSingleThreadRandomWalk(graphDb, highestNodeId, noOfSteps, output);

        runtimes[1] = doMultiThreadRandomWalk(graphDb,highestNodeId,noOfSteps, output);

        return runtimes;

    }

    private static long doMultiThreadRandomWalk(GraphDatabaseService graphDb, int highestNodeId, int noOfSteps, boolean output){

        // INIT
        Map<Thread,MyAlgorithmBaseRunnable> map = new HashMap<>();
        for(int i=0;i<NUMBER_OF_THREADS;i++){

            MyAlgorithmBaseRunnable rw;
            if(NEWSPI){
                rw = new RandomWalkAlgorithmRunnableNewSPI(RANDOMWALKRANDOM,
                        graphDb,highestNodeId, noOfSteps/NUMBER_OF_THREADS, output);
            } else{
                rw = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM,
                        graphDb,highestNodeId, noOfSteps/NUMBER_OF_THREADS, output);
            }
            Thread ttemp = rw.getThread();

            if(output){
                ttemp.setName(rw.getClass().getSimpleName() +"_"+ i);
            } else{
                ttemp.setName("WarmUp:" +rw.getClass().getSimpleName() +"_"+ i);
            }

            map.put(ttemp, rw);
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

                t = null; // suggestion for garbage collector

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        map = null; // suggestion for garbage collector

        return elapsedTime;
    }

    private static long doSingleThreadRandomWalk(GraphDatabaseService graphDb, int highestNodeId, int noOfSteps, boolean output){

        MyAlgorithmBaseRunnable rwst;
        if(NEWSPI){
            rwst = new RandomWalkAlgorithmRunnableNewSPI(RANDOMWALKRANDOM, graphDb,highestNodeId,noOfSteps, output);
        } else{
            rwst = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM, graphDb,highestNodeId,noOfSteps, output);

        }
        Thread thr = rwst.getThread();
        thr.setName(rwst.getClass().getSimpleName() + "_single");
        thr.start();
        try {
            thr.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
                try {
                    graphDb.shutdown();
                } catch (Exception e) {
                    graphDb.shutdown();
                }
            }
        });
    }


    public static boolean writeResultsOut(GraphDatabaseService graphDb, int highestNodeid){

        int sizeKeySet = resultCounter.keySet().size();
        int partOfData = sizeKeySet/NUMBER_OF_THREADS;  // LAST ONE TAKES MORE!

        List<Thread> listOfThreads = new ArrayList<>();  // TODO: CHANGE TO ARRAYS

        int startIndex =0;
        int endIndex = partOfData;
        for(int i=0;i<NUMBER_OF_THREADS;i++){

            NeoWriter neoWriter = new NeoWriter(PROP_ID,graphDb,startIndex,endIndex);
            Thread t = neoWriter.getThread();
            t.setName(neoWriter.getClass().getSimpleName() + "_" + i);
            listOfThreads.add(t);

            // new indexes for next round
            startIndex = endIndex;
            endIndex = endIndex + partOfData;

            if(i==NUMBER_OF_THREADS-2){
                endIndex= sizeKeySet;
            }

        }

        for(Thread t:listOfThreads){
            t.start();
        }

        for(Thread t:listOfThreads){
            try {
                t.join();
                t = null;  // just to try it
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        System.out.println("Done Writing");

        listOfThreads = null;


        return true;
    }

    private static void prepaireResultMapAndCounter(GraphDatabaseService graphDb, int nodeIDhigh){

        Iterator<Node> it = DBUtils.getIteratorForAllNodes(graphDb);

        resultCounter = new HashMap<>(nodeIDhigh*2,1f);

        while(it.hasNext()){
            resultCounter.put(it.next().getId(),new AtomicInteger(0));
        }


        keySetOfResultCounter = resultCounter.keySet().toArray();  // should only be called once!
    }


}
