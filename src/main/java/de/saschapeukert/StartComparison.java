package de.saschapeukert;

import com.google.common.base.Stopwatch;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.ConnectedComponentsSingleThreadAlgorithm;
import de.saschapeukert.Algorithms.Impl.RandomWalk.RandomWalkAlgorithmRunnable;
import de.saschapeukert.Algorithms.Impl.RandomWalk.RandomWalkAlgorithmRunnableNewSPI;
import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Database.NeoWriter;
import org.HdrHistogram.Histogram;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    private  static String DB_PATH;
    // Server: /mnt/flash2/neo4j-enterprise-2.3.0-M02/data/graph.db
    // SSD: C:\\BelegDB\\neo4j-enterprise-2.3.0-M02\\data\\graph.db
    // Meine HDD: E:\\Users\\Sascha\\Documents\\GIT\\BelegParallelAlgorithmsNeo4j\\testDB\\graph.db

    private static int OPERATIONS;
    private static int NUMBER_OF_THREADS;
    private static int NUMBER_OF_RUNS; //Minimum: 1
    private static final int RANDOMWALKRANDOM = 20;  // Minimum: 1
    private static  int WARMUPTIME; // in seconds
    private static  boolean NEWSPI;
    private static  String PROP_NAME;
    private static int PROP_ID;
    private static String PAGECACHE;
    private static String ALGORITHM;
    private static String WRITE;

    private static Map<Long,AtomicInteger> resultCounter;
    private static Object[] keySetOfResultCounter;

    private static final Histogram histogram = new Histogram(3600000000000L, 3);


    public static void main(String[] args)  {

        readParameters(args);

        System.out.println("Start: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

        System.out.println("I will start the "+ ALGORITHM +" with: " + NUMBER_OF_THREADS +
               " Thread(s).\n");

        // Open connection to DB
        GraphDatabaseService graphDb = DBUtils.getGraphDb(DB_PATH,PAGECACHE);

        /*GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder(DB_PATH);

        builder.setConfig(ClusterSettings.server_id, "1");
        builder.setConfig(HaSettings.ha_server, "localhost:6001");
        builder.setConfig(HaSettings.slave_only, Settings.FALSE);
        builder.setConfig(ClusterSettings.cluster_server, "localhost:5001");
        builder.setConfig(ClusterSettings.initial_hosts, "localhost:5001,localhost:5002");

        GraphDatabaseService graphDb = builder.newGraphDatabase();
        */


        int nodeIDhigh = DBUtils.getHighestNodeID(graphDb);
        if(nodeIDhigh==0){
            System.out.println("No Nodes/DB found at this Path:" + DB_PATH);
            System.out.println("Abort.");
            return;
        }

        int highestPropertyKey = DBUtils.getHighestPropertyID(graphDb);


        Transaction t = DBUtils.openTransaction(graphDb);
            prepaireResultMapAndCounter(graphDb, nodeIDhigh);
        DBUtils.closeTransactionWithSuccess(t);

        System.out.println("~ " + nodeIDhigh + " Nodes");


        if(WRITE.equals("Write")){

            PROP_ID = DBUtils.GetPropertyID(PROP_NAME, highestPropertyKey, graphDb);

            if(PROP_ID==-1){
                // ERROR Handling
                System.out.println("Something went wrong while looking up or creating the PropertyID. See Stacktrace for Answers.");
                return;
            }
        }

        WarmUp(graphDb, nodeIDhigh, WARMUPTIME);

        System.out.println("");

        Stopwatch timeOfComparision = Stopwatch.createStarted();


        switch (ALGORITHM){
            case "RW":
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
            writeResultsOut(graphDb);

        //java.awt.Toolkit.getDefaultToolkit().beep();
        System.out.println("End: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));

       // graphDb.shutdown();

    }

    private static void readParameters(String[] args) {
        // READING THE INPUTPARAMETER
        try {
            ALGORITHM = args[0];
            OPERATIONS = Integer.valueOf(args[1]);
            NUMBER_OF_RUNS = Integer.valueOf(args[2]);
            NUMBER_OF_THREADS = Integer.valueOf(args[3]);
            WARMUPTIME = Integer.valueOf(args[4]);
            NEWSPI = args[5].equals("true");
            PROP_NAME = args[6];
            PAGECACHE= args[7];
            DB_PATH = args[8];
            WRITE = args[9];

        } catch(Exception e){

            System.out.println("Not enough input parameter.");
            System.out.println("You have to supply: AlgorithmName OperationNumber Number_of_Threads WarmUpTime_in_s " +
                    "NewSPI_bool PropertyName PageCache(String)_in_G/M/K DB-Path Write/NoWrite");
            System.exit(1);
        }
    }

    /**
     * WarmUp
     * @param graphDb
     * @param highestNodeId
     * @param secs
     */
    private static void WarmUp(GraphDatabaseService graphDb, int highestNodeId, int secs){
        System.out.println("Starting WarmUp.");

        Stopwatch timer = Stopwatch.createStarted();

        while (timer.elapsed(TimeUnit.SECONDS) < secs) {
                doMultiThreadRandomWalk(graphDb,highestNodeId,100, false);

            }

        timer.stop();

        System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
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

        System.out.println(ConnectedSingle.getResults());    //TODO REMOVE, JUST FOR DEBUG

        return ConnectedSingle.timer.elapsed(TimeUnit.MILLISECONDS);

    }


    private static long doMultiThreadRandomWalk(GraphDatabaseService graphDb, int highestNodeId, int noOfSteps, boolean output){

        // INIT
        List<MyAlgorithmBaseRunnable> list = new ArrayList<>(NUMBER_OF_THREADS);

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        for(int i=0;i<NUMBER_OF_THREADS;i++){

            MyAlgorithmBaseRunnable rw;
            if(NEWSPI){
                rw = new RandomWalkAlgorithmRunnableNewSPI(RANDOMWALKRANDOM,
                        graphDb,highestNodeId, noOfSteps/NUMBER_OF_THREADS, output);
            } else{
                rw = new RandomWalkAlgorithmRunnable(RANDOMWALKRANDOM,
                        graphDb,highestNodeId, noOfSteps/NUMBER_OF_THREADS, output);
            }
            executor.execute(rw);
            list.add(rw);

        }

        waitForExecutorToFinishAll(executor);

        long elapsedTime=0;

        for(MyAlgorithmBaseRunnable runnable : list){

          // only the longest running thread time is of importance
          if(elapsedTime<runnable.timer.elapsed(TimeUnit.MICROSECONDS)){
             elapsedTime =runnable.timer.elapsed(TimeUnit.MICROSECONDS);
          }

          runnable = null; // suggestion for garbage collector

        }

        return elapsedTime;
    }


    private static boolean writeResultsOut(GraphDatabaseService graphDb){

        int sizeKeySet = resultCounter.keySet().size();
        int partOfData = sizeKeySet/NUMBER_OF_THREADS;  // LAST ONE TAKES MORE!

        int startIndex =0;
        int endIndex = partOfData;
        //ThreadPoolExecutor executor = new ThreadPoolExecutor(NUMBER_OF_THREADS,NUMBER_OF_THREADS,0L,TimeUnit.NANOSECONDS,new ArrayBlockingQueue<Runnable>(1));
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        for(int i=0;i<NUMBER_OF_THREADS;i++){

            NeoWriter neoWriter = new NeoWriter(PROP_ID,graphDb,startIndex,endIndex);
            executor.execute(neoWriter);

            // new indexes for next round
            startIndex = endIndex;
            endIndex = endIndex + partOfData;

            if(i==NUMBER_OF_THREADS-2){
                endIndex= sizeKeySet;
            }

        }
        boolean check = waitForExecutorToFinishAll(executor);

        System.out.println("Done Writing");

        return check;
    }

    private static void prepaireResultMapAndCounter(GraphDatabaseService graphDb, int nodeIDhigh){

        Iterator<Node> it = DBUtils.getIteratorForAllNodes(graphDb);

        resultCounter = new HashMap<>(nodeIDhigh*2,1f);

        while(it.hasNext()){
            resultCounter.put(it.next().getId(),new AtomicInteger(0));
        }


        keySetOfResultCounter = resultCounter.keySet().toArray();  // should only be called once!
    }

    private static boolean waitForExecutorToFinishAll(ExecutorService executor){
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static Object getObjInResultCounterKeySet(int pos){
        return keySetOfResultCounter[pos];
    }
    public static int incrementResultCounterforId(long id){
       return getResultCounterforId(id).incrementAndGet();
    }

    public static AtomicInteger getResultCounterforId(long id){
        return resultCounter.get(id);
    }
    public static boolean resultCounterContainsKey(long id){
        return resultCounter.containsKey(id);
    }
    public static void putIntoResultCounter(long id, AtomicInteger value){
        resultCounter.put(id,value);
    }

    public static Iterator<Long> getIteratorforKeySetOfResultCounter(){
        return resultCounter.keySet().iterator();
    }

}
