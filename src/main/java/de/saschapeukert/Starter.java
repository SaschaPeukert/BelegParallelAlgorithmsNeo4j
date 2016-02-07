package de.saschapeukert;

import com.google.common.base.Stopwatch;
import de.saschapeukert.Algorithms.Abst.MyAlgorithmBaseCallable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.CCAlgorithmType;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.STConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.RandomWalk.RandomWalkCoreApiAlgorithmCallable;
import de.saschapeukert.Algorithms.Impl.RandomWalk.RandomWalkKernelApiAlgorithmCallable;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Database.NeoWriterRunnable;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.IntCountsHistogram;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents the Starter.
 * It's function is described in Section 4.1 of my study project document.
 * <br>
 * Created by Sascha Peukert on 04.08.2015.
 */
public class Starter {

    private  static String DB_PATH;
    private static int OPERATIONS;
    public static int NUMBER_OF_THREADS; //Runtime.getRuntime().availableProcessors();
    private static int NUMBER_OF_RUNS; //Minimum: 1
    public static final int RANDOMWALKRANDOM = 20;  // Minimum: 1
    private static  boolean NEWSPI;
    private static  String PROP_NAME;
    private static int PROP_ID;
    private static String PAGECACHE;
    private static String ALGORITHM;
    private static String WRITE;
    private static Map<Long,AtomicLong> resultCounter;
    private static Object[] keySetOfResultCounter;
    private static Histogram histogram;

    public static int BATCHSIZE;//  = 100000;
    private static int writeBATCHSIZE = 100000;

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
            prepaireResultMapAndCounter();
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

        System.out.println("");
        Stopwatch timeOfComparision = Stopwatch.createStarted();

        switch (ALGORITHM){
            case "RW":
                calculateRandomWalk_new(NUMBER_OF_RUNS);
                break;
            case "WCC":
                calculateConnectedComponents(
                         NUMBER_OF_RUNS,
                        CCAlgorithmType.WEAK);
                break;
            case "SCC":
                calculateConnectedComponents(
                        NUMBER_OF_RUNS,
                        CCAlgorithmType.STRONG);
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

    }

    private static void readParameters(String[] args) {
        // READING THE INPUTPARAMETER
        try {
            ALGORITHM = args[0];
            OPERATIONS = Integer.valueOf(args[1]);
            BATCHSIZE = OPERATIONS;
            NUMBER_OF_RUNS = Integer.valueOf(args[2]);
            NUMBER_OF_THREADS = Integer.valueOf(args[3]);
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
                doMultiThreadRandomWalk(100);
            }
        timer.stop();
        System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
    }

    private static void SkippingPagesWarmUp(){
        System.out.println("Starting WarmUp.");
        Stopwatch timer = Stopwatch.createStarted();

        int sizeOfDbPage_byte = 8000;
        int sizeOfNodeRecord_byte=15;
        int sizeOfRelationshipRecord_byte = 34;

        DBUtils db=DBUtils.getInstance("","");
        Transaction tx =db.openTransaction();
        for(int i=0;i<=db.highestNodeKey;i=i+(sizeOfDbPage_byte/sizeOfNodeRecord_byte)){
            db.loadNode(i);
        }
        for(int i=0;i<=db.highestRelationshipKey;i=i+(sizeOfDbPage_byte/sizeOfRelationshipRecord_byte)){
            db.loadRelationship(i);
        }
        db.closeTransactionWithSuccess(tx);
        timer.stop();
        System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
    }

    private static void calculateConnectedComponents(int runs, CCAlgorithmType type){
        for(int i=0;i<runs;i++){
            System.out.println("Now doing run " + (i + 1));
            histogram.recordValue(doConnectedComponentsRun( type));
        }
        System.out.println("");
        System.out.println("Times of CC with " + NUMBER_OF_THREADS + " Threads:");
        histogram.outputPercentileDistribution(System.out, 1.00);
    }

    private static void calculateRandomWalk_new(int numberOfRunsPerStep){
        for(int i=0;i<numberOfRunsPerStep;i++){
            System.out.println("Now doing run " + (i + 1));
            long run = doMultiThreadRandomWalk(OPERATIONS);
            histogram.recordValue(run);
        }
        System.out.println("");
        System.out.println("Times of RW with " + NUMBER_OF_THREADS +" Threads:");
        histogram.outputPercentileDistribution(System.out, 1.00);
    }

    /**
     *
     * @param type
     * @return elapsed time as MILLISECONDS!
     */
    private static long doConnectedComponentsRun(CCAlgorithmType type){
        STConnectedComponentsAlgo callable;
        if(NUMBER_OF_THREADS>1) {
            callable = new MTConnectedComponentsAlgo(
                    type,TimeUnit.MILLISECONDS);
        } else{
            // Easter Egg?!
            if(OPERATIONS==-1){
                callable = new MTConnectedComponentsAlgo(
                        type,TimeUnit.MILLISECONDS);
            } else{
                callable = new STConnectedComponentsAlgo(
                        type,TimeUnit.MILLISECONDS);
            }
        }
        ExecutorService ex = Executors.newFixedThreadPool(1);
        long ret = -10000;
        try {
            ret = (long)(ex.submit(callable).get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        //System.out.println(callable.getResults());    //TODO REMOVE, JUST FOR DEBUG
        Utils.waitForExecutorToFinishAll(ex);
        return ret; // ERROR
    }

    private static long doMultiThreadRandomWalk(int noOfSteps){
        // INIT
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        List<Future<Long>> list = new ArrayList<>();

        for(int i=0;i<NUMBER_OF_THREADS;i++){
            MyAlgorithmBaseCallable rw;
            if(NEWSPI){
                rw = new RandomWalkKernelApiAlgorithmCallable(
                        noOfSteps/NUMBER_OF_THREADS,TimeUnit.MICROSECONDS);
            } else{
                rw = new RandomWalkCoreApiAlgorithmCallable(
                        noOfSteps/NUMBER_OF_THREADS,TimeUnit.MICROSECONDS);
            }
            list.add(executor.submit(rw));
        }
        long elapsedTime=0;

        for(int i=0;i<NUMBER_OF_THREADS;i++){
            try {
                long time = list.get(i).get();
                if(elapsedTime<time){
                    elapsedTime =time;
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        Utils.waitForExecutorToFinishAll(executor);
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
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        int maxIndex = resultCounter.keySet().size();
        List<Future> futureList = new ArrayList<>();
        for(int i=0;i<maxIndex;i+=writeBATCHSIZE){
            NeoWriterRunnable neoWriterRunnable;
            if(i+writeBATCHSIZE>=maxIndex){
                neoWriterRunnable = new NeoWriterRunnable(PROP_ID,i,maxIndex,DBUtils.getInstance("", ""));
            } else{
                neoWriterRunnable = new NeoWriterRunnable(PROP_ID,i,i+BATCHSIZE,DBUtils.getInstance("", ""));
            }
            futureList.add(executor.submit(neoWriterRunnable));
        }

        for (Future future : futureList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        boolean check = Utils.waitForExecutorToFinishAll(executor);
        System.out.println("Done Writing. (" + futureList.size() + "T)");
        return check;
    }

    private static void prepaireResultMapAndCounter(){
        Iterator<Node> it = DBUtils.getInstance("", "").getIteratorForAllNodes();
        resultCounter = new HashMap<>(0,1f);

        while(it.hasNext()){
            resultCounter.put(it.next().getId(),new AtomicLong(0));
        }
        keySetOfResultCounter = resultCounter.keySet().toArray();  // should only be called once!
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
