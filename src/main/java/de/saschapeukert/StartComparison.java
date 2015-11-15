package de.saschapeukert;

import com.google.common.base.Stopwatch;
import de.saschapeukert.Algorithms.Abst.MyAlgorithmBaseRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.CCAlgorithmType;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.STConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.RandomWalk.RandomWalkAlgorithmRunnable;
import de.saschapeukert.Algorithms.Impl.RandomWalk.RandomWalkAlgorithmRunnableNewSPI;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Database.NeoWriter;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.IntCountsHistogram;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Sascha Peukert on 04.08.2015.
 */
public class StartComparison {

    private  static String DB_PATH;
    // Server: /mnt/flash2/neo4j-enterprise-2.3.0-M02/data/graph.db
    // SSD: C:\\BelegDB\\neo4j-enterprise-2.3.0-M02\\data\\graph.db
    // Meine HDD: E:\\Users\\Sascha\\Documents\\GIT\\BelegParallelAlgorithmsNeo4j\\testDB\\graph.db
    private static int OPERATIONS;
    public static int NUMBER_OF_THREADS;
    private static int NUMBER_OF_RUNS; //Minimum: 1
    public static final int RANDOMWALKRANDOM = 20;  // Minimum: 1
    //private static  int WARMUPTIME; // in seconds
    private static  boolean NEWSPI;
    private static  String PROP_NAME;
    private static int PROP_ID;
    private static String PAGECACHE;
    private static String ALGORITHM;
    private static String WRITE;
    private static Map<Long,AtomicLong> resultCounter;
    private static Object[] keySetOfResultCounter;
    private static Histogram histogram;
    ;
    public static void main(String[] args)  {
        readParameters(args);

        System.out.println("Start: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
        System.out.println("I will start the "+ ALGORITHM +" with: " + NUMBER_OF_THREADS +
               " Thread(s).\n");

        // Open connection to DB
        DBUtils db = DBUtils.getInstance(DB_PATH, PAGECACHE);
        if(db.highestNodeKey==0){
            System.out.println("No Nodes/DB found at this Path:" + DB_PATH);
            System.out.println("Abort.");
            return;
        }
        histogram = new Histogram(3600000000000L, 3);
        Transaction t = db.openTransaction();
            prepaireResultMapAndCounter(db.highestNodeKey);
        db.closeTransactionWithSuccess(t);
        System.out.println("~ " + db.highestNodeKey + " Nodes");
        System.out.println("~ " + db.highestRelationshipKey + " Relationships");

        if(WRITE.equals("Write")){
            PROP_ID = db.GetPropertyID(PROP_NAME);
            if(PROP_ID==-1){
                // ERROR Handling
                System.out.println("Something went wrong while looking up or creating the PropertyID. See Stacktrace for Answers.");
                return;
            }
        }
        SkippingPagesWarmUp();
        //WarmUp(WARMUPTIME);
        System.out.println("");
        Stopwatch timeOfComparision = Stopwatch.createStarted();

        switch (ALGORITHM){
            case "RW":
                calculateRandomWalk_new(NUMBER_OF_RUNS, true);
                break;
            case "WCC":
                calculateConnectedComponents(
                         NUMBER_OF_RUNS,
                        CCAlgorithmType.WEAK, true);
                break;
            case "SCC":
                calculateConnectedComponents(
                        NUMBER_OF_RUNS,
                        CCAlgorithmType.STRONG, true);
                break;
            case "DegreeStats":
                doGetDegreeStatistics(db);
                break;
            default:
                System.out.println("Error: Unknown Algorithm.");
                return;
        }
        timeOfComparision.stop();
        System.out.println("\nCalculations done in: " + timeOfComparision.elapsed(TimeUnit.SECONDS) + "s (+ WarmUp time)");
        if(WRITE.equals("Write"))
            writeResultsOut();

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
            //WARMUPTIME = Integer.valueOf(args[4]);
            NEWSPI = args[4].equals("true");
            PROP_NAME = args[5];
            PAGECACHE= args[6];
            DB_PATH = args[7];
            WRITE = args[8];
        } catch(Exception e){
            System.out.println("Not enough input parameter.");
            System.out.println("You have to supply: AlgorithmName OperationNumber Number_of_Threads " +
                    "NewSPI_bool PropertyName PageCache(String)_in_G/M/K DB-Path Write/NoWrite");
            System.exit(1);
        }
    }

    /**
     * WarmUp
     * @param secs
     */
    private static void WarmUp( int secs){
        System.out.println("Starting WarmUp.");
        Stopwatch timer = Stopwatch.createStarted();

        while (timer.elapsed(TimeUnit.SECONDS) < secs) {
                doMultiThreadRandomWalk(100, false);
            }
        timer.stop();
        System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
    }

    private static void SkippingPagesWarmUp(){
        System.out.println("Starting WarmUp.");
        Stopwatch timer = Stopwatch.createStarted();

        DBUtils db=DBUtils.getInstance("","");
        Transaction tx =db.openTransaction();
        for(int i=0;i<=db.highestNodeKey;i=i+15){
            db.loadNode(i);
        }
        for(int i=0;i<=db.highestRelationshipKey;i=i+15){
            db.loadRelationship(i);
        }
        db.closeTransactionWithSuccess(tx);
        timer.stop();
        System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
    }

    private static void calculateConnectedComponents(int runs, CCAlgorithmType type, boolean output){
        for(int i=0;i<runs;i++){
            System.out.println("Now doing run " + (i + 1));
            histogram.recordValue(doConnectedComponentsRun( type, output));
        }
        System.out.println("");
        System.out.println("Times of CC with " + NUMBER_OF_THREADS + " Threads:");
        histogram.outputPercentileDistribution(System.out, 1.00);
    }

    private static void calculateRandomWalk_new(int numberOfRunsPerStep, boolean output){
        for(int i=0;i<numberOfRunsPerStep;i++){
            System.out.println("Now doing run " + (i + 1));
            long run = doMultiThreadRandomWalk(OPERATIONS, output);
            histogram.recordValue(run);
        }
        System.out.println("");
        System.out.println("Times of RW with " + NUMBER_OF_THREADS +" Threads:");
        histogram.outputPercentileDistribution(System.out, 1.00);
    }

    /**
     *
     * @param type
     * @param output
     * @return elapsed time as MILLISECONDS!
     */
    private static long doConnectedComponentsRun(CCAlgorithmType type, boolean output){
        STConnectedComponentsAlgo runnable;
        if(NUMBER_OF_THREADS>1) {
            runnable = new MTConnectedComponentsAlgo(
                    type, output);
        } else{
            // Easter Egg?!
            if(OPERATIONS==-1){
                runnable = new MTConnectedComponentsAlgo(
                        type, output);
            } else{
                runnable = new STConnectedComponentsAlgo(
                        type, output);
            }
        }
        Thread t = new Thread(runnable);
        t.setName("ConnectedComponentsAlgo");
        t.start();
        try {
            t.join();
            t = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //System.out.println(runnable.getResults());    //TODO REMOVE, JUST FOR DEBUG
        return runnable.timer.elapsed(TimeUnit.MILLISECONDS);
    }

    private static long doMultiThreadRandomWalk(int noOfSteps, boolean output){
        // INIT
        List<MyAlgorithmBaseRunnable> list = new ArrayList<>(NUMBER_OF_THREADS);
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        for(int i=0;i<NUMBER_OF_THREADS;i++){
            MyAlgorithmBaseRunnable rw;
            if(NEWSPI){
                rw = new RandomWalkAlgorithmRunnableNewSPI(
                        noOfSteps/NUMBER_OF_THREADS, output);
            } else{
                rw = new RandomWalkAlgorithmRunnable(
                        noOfSteps/NUMBER_OF_THREADS, output);
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

        }
        return elapsedTime;
    }

    private static void doGetDegreeStatistics(DBUtils db){
        Map<Integer, Integer> mapInDegreeCounter= new HashMap<>();
        Map<Integer, Integer> mapOutDegreeCounter= new HashMap<>();
        Map<Integer, Integer> mapDegreeCounter= new HashMap<>();

        IntCountsHistogram histogram_in = new IntCountsHistogram(0);
        IntCountsHistogram histogram_out = new IntCountsHistogram(0);
        IntCountsHistogram histogram_sum = new IntCountsHistogram(0);

        Iterator<Long> it = resultCounter.keySet().iterator();
        Transaction tx =db.openTransaction();
        while(it.hasNext()){
            Long l = it.next();
            int in =db.getDegree(l, Direction.INCOMING);
            int out = db.getDegree(l, Direction.OUTGOING);
            incrementMapCounter(mapInDegreeCounter,in);
            incrementMapCounter(mapOutDegreeCounter,out);
            incrementMapCounter(mapDegreeCounter,in+out);
        }
        db.closeTransactionWithSuccess(tx);

        // Raw Output
        System.out.println("IN:");
        for(int i:mapInDegreeCounter.keySet()){
            System.out.println(i + ";" + mapInDegreeCounter.get(i));
        }
        System.out.println("");
        System.out.println("OUT:");
        for(int i:mapOutDegreeCounter.keySet()){
            System.out.println(i + ";" + mapOutDegreeCounter.get(i));
        }
        System.out.println("");
        System.out.println("SUM:");
        for(int i:mapDegreeCounter.keySet()){
            System.out.println(i + ";" + mapDegreeCounter.get(i));
        }
        System.out.println("");

        // Histogram Output
        for(int i:mapInDegreeCounter.keySet()){
            histogram_in.recordValueWithCount((long)i ,(long) mapInDegreeCounter.get(i));
        }
        for(int i:mapOutDegreeCounter.keySet()){
            histogram_out.recordValueWithCount((long)i ,(long) mapOutDegreeCounter.get(i));
        }
        for(int i:mapDegreeCounter.keySet()){
            histogram_sum.recordValueWithCount((long)i ,(long) mapDegreeCounter.get(i));
        }
        System.out.println("In-Degrees:");
        histogram_in.outputPercentileDistribution(System.out, 1.00);
        System.out.println("");

        System.out.println("Out-Degrees:");
        histogram_out.outputPercentileDistribution(System.out, 1.00);
        System.out.println("");

        System.out.println("Degrees:");
        histogram_sum.outputPercentileDistribution(System.out, 1.00);

    }

    private static void incrementMapCounter(Map<Integer, Integer> map, int id){
        if(map.containsKey(id)){
            int value = map.get(id);
            value++;
            map.put(id,value);
        } else{
            map.put(id,1);
        }
    }

    private static boolean writeResultsOut(){
        int sizeKeySet = resultCounter.keySet().size();
        int partOfData = sizeKeySet/NUMBER_OF_THREADS;  // LAST ONE TAKES MORE!

        int startIndex =0;
        int endIndex = partOfData;
        //ThreadPoolExecutor executor = new ThreadPoolExecutor(NUMBER_OF_THREADS,NUMBER_OF_THREADS,0L,TimeUnit.NANOSECONDS,new ArrayBlockingQueue<Runnable>(1));
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        for(int i=0;i<NUMBER_OF_THREADS;i++){
            NeoWriter neoWriter = new NeoWriter(PROP_ID,startIndex,endIndex);
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

    private static void prepaireResultMapAndCounter( long nodeIDhigh){
        Iterator<Node> it = DBUtils.getInstance("", "").getIteratorForAllNodes();
        resultCounter = new HashMap<>(0,1f);

        while(it.hasNext()){
            resultCounter.put(it.next().getId(),new AtomicLong(0));
        }
        keySetOfResultCounter = resultCounter.keySet().toArray();  // should only be called once!
    }

    public static boolean waitForExecutorToFinishAll(ExecutorService executor){
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
    public static long incrementResultCounterforId(long id){
       return getResultCounterforId(id).incrementAndGet();
    }

    public static AtomicLong getResultCounterforId(long id){
        return resultCounter.get(id);
    }
    public static boolean resultCounterContainsKey(long id){
        return resultCounter.containsKey(id);
    }
    public static synchronized void putIntoResultCounter(long id, AtomicLong value){
        resultCounter.put(id,value);
    }

    public static Iterator<Long> getIteratorforKeySetOfResultCounter(){
        return resultCounter.keySet().iterator();
    }

}
