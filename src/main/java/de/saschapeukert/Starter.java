package de.saschapeukert;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Stopwatch;
import de.saschapeukert.algorithms.abst.MyAlgorithmBaseCallable;
import de.saschapeukert.algorithms.impl.connected.components.CCAlgorithmType;
import de.saschapeukert.algorithms.impl.connected.components.MTConnectedComponentsAlgo;
import de.saschapeukert.algorithms.impl.connected.components.STConnectedComponentsAlgo;
import de.saschapeukert.algorithms.impl.randomwalk.RandomWalkCoreApiAlgorithmCallable;
import de.saschapeukert.algorithms.impl.randomwalk.RandomWalkKernelApiAlgorithmCallable;
import de.saschapeukert.database.DBUtils;
import de.saschapeukert.database.NeoWriterRunnable;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.IntCountsHistogram;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
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
    private static LongObjectHashMap<AtomicLong> resultCounter;
    private static long[] keySetOfResultCounter;
    private static Histogram histogram;

    public static int BATCHSIZE;//  = 100000;
    private static int writeBATCHSIZE = 100000;

    private static double parallelTimes_percent;

    public static boolean unittest = false;
    private static boolean mt1 = false;

    public static void main(String[] args)  {
        readParameters(args);

        parallelTimes_percent = 0;

        System.out.println("Start: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
        System.out.println("I will start the "+ ALGORITHM +" with: " + NUMBER_OF_THREADS +
               " Thread(s).\n");

        // Open connection to DB
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH))
                .setConfig(GraphDatabaseSettings.pagecache_memory, PAGECACHE)
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "false")  // to get rid of all those neostore.trasaction.db ... files
                .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
                .newGraphDatabase();
        DBUtils db = new DBUtils(graphDb);
        db.registerShutdownHook();  // important to do

        if(db.highestNodeKey==0){
            System.out.println("No Nodes/DB found at this Path:" + DB_PATH);
            System.out.println("Abort.");
            return;
        }
        histogram = new Histogram(3600000000000L, 3);
        Transaction t = db.openTransaction();
            prepaireResultMapAndCounter(db);
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
        SkippingPagesWarmUp(false, db);

        if(NUMBER_OF_THREADS==-1){
            mt1=true;
        }

        System.out.println("");
        Stopwatch timeOfComparision = Stopwatch.createStarted();

        switch (ALGORITHM){
            case "RW":
                calculateRandomWalk_new(NUMBER_OF_RUNS,false, db);
                break;
            case "WCC":
                calculateConnectedComponents(
                         NUMBER_OF_RUNS,
                        CCAlgorithmType.WEAK, db);
                break;
            case "SCC":
                calculateConnectedComponents(
                        NUMBER_OF_RUNS,
                        CCAlgorithmType.STRONG, db);
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
        System.out.println("ca. " + (parallelTimes_percent /NUMBER_OF_RUNS) + "% of it in parallel");
        if(WRITE.equals("Write"))
            writeResultsOut(db);

        //java.awt.Toolkit.getDefaultToolkit().beep();
        System.out.println("End: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS\n").format(new Date()));

        if(!unittest) {
            System.exit(0);
        } else{
            db.shutdownDB();
        }
    }

    public static long[] mainAsExtension(String[]args, DBUtils db)  {

        // common args
        String algorithm = args[0];
        String propertyName = args[1];
        BATCHSIZE = Integer.valueOf(args[2]);

        if(args[3].equals("auto")){
            NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors();
        } else{
            NUMBER_OF_THREADS = Integer.valueOf(args[3]);
        }


        if(!args[4].equals("")){
            writeBATCHSIZE = Integer.valueOf(args[4]);
            // otherwise default 100,000
        }

        long[] result = new long[2];
        result[0] =0;

        Transaction t = db.openTransaction();
        prepaireResultMapAndCounter(db);
        db.closeTransactionWithSuccess(t);

        PROP_ID = db.GetPropertyID(propertyName);
        if(PROP_ID==-1){
                // ERROR Handling
                System.out.println("Something went wrong while looking up or creating the PropertyID. See Stacktrace for Answers.");
                result[0] = -1;
                return result;
        }

        switch (algorithm){
            case "RW":
                NEWSPI = args[6].equals("true");
                Integer number_of_steps = Integer.valueOf(args[5]);
                result[0] = doRandomWalk(number_of_steps,true, db);
                break;
            case "WCC":
                result[0] = doConnectedComponentsRun_Extension(CCAlgorithmType.WEAK, db);
                break;
            case "SCC":
                result[0] = doConnectedComponentsRun_Extension(CCAlgorithmType.STRONG, db);
                break;

            default:
                //System.out.println("Error: Unknown Algorithm.");
                result[0] = -2;
                return result;
        }
        result[1] =writeResultsOut_Extension(db);
        return result;
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

    public static void SkippingPagesWarmUp(boolean extensionContext, DBUtils db){
        if(!extensionContext)
            System.out.println("Starting WarmUp.");
        Stopwatch timer = Stopwatch.createStarted();

        int sizeOfDbPage_byte = 8000;
        int sizeOfNodeRecord_byte=15;
        int sizeOfRelationshipRecord_byte = 34;

        Transaction tx = db.openTransaction();
        for(int i=0;i<=db.highestNodeKey;i=i+(sizeOfDbPage_byte/sizeOfNodeRecord_byte)){
            db.loadNode(i);
        }
        for(int i=0;i<=db.highestRelationshipKey;i=i+(sizeOfDbPage_byte/sizeOfRelationshipRecord_byte)){
            db.loadRelationship(i);
        }
        db.closeTransactionWithSuccess(tx);

        timer.stop();
        if(!extensionContext)
            System.out.println("WarmUp finished after " + timer.elapsed(TimeUnit.SECONDS) + "s.");
    }

    private static void calculateConnectedComponents(int runs, CCAlgorithmType type, DBUtils db){
        for(int i=0;i<runs;i++){
            System.out.println("Now doing run " + (i + 1));
            histogram.recordValue(doConnectedComponentsRun( type, db));
        }
        System.out.println("");
        System.out.println("Times of CC with " + NUMBER_OF_THREADS + " Threads:");
        histogram.outputPercentileDistribution(System.out, 1.00);
    }

    private static void calculateRandomWalk_new(int numberOfRunsPerStep, boolean extensionContext, DBUtils db){
        float visitedNodes = 0;
        for(int i=0;i<numberOfRunsPerStep;i++){
            System.out.println("Now doing run " + (i + 1));
            long run = doRandomWalk(OPERATIONS,extensionContext, db);
            histogram.recordValue(run);
        }

        Iterator<ObjectCursor<AtomicLong>> col = resultCounter.values().iterator();
        while(col.hasNext()){
            if(col.next().value.get()>0){
                visitedNodes++;
            }
        }
        visitedNodes = (100*visitedNodes)/resultCounter.size();

        System.out.println("");
        System.out.println("Percentage of visited Nodes: " + visitedNodes + "%");
        System.out.println("Times of RW with " + NUMBER_OF_THREADS +" Threads:");
        histogram.outputPercentileDistribution(System.out, 1.00);
    }

    /**
     *
     * @param type
     * @return elapsed time as MILLISECONDS!
     */
    private static long doConnectedComponentsRun(CCAlgorithmType type, DBUtils db){
        STConnectedComponentsAlgo callable;
        if(NUMBER_OF_THREADS>1) {
            callable = new MTConnectedComponentsAlgo(
                    type,TimeUnit.MILLISECONDS, db);
        } else{
            // Easter Egg?!
            if(mt1){
                NUMBER_OF_THREADS = 1;
                callable = new MTConnectedComponentsAlgo(
                        type,TimeUnit.MILLISECONDS, db);
            } else{
                callable = new STConnectedComponentsAlgo(
                        type,TimeUnit.MILLISECONDS, db);
            }
        }
        ExecutorService ex = Executors.newFixedThreadPool(1);
        long ret = -10000;
        try {
            ret = (long)(ex.submit(callable).get());
            if(mt1==false){
                double percent = ((100/(double)ret)*callable.parallelTime);
                parallelTimes_percent = parallelTimes_percent +percent;
                System.out.println("duration: " + ret + "ms");
                System.out.println("parallel duration: " + callable.parallelTime + "ms");
                System.out.println("parallel %: " + percent);

            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        if(mt1){
            NUMBER_OF_THREADS=-1;
        }

        Utils.waitForExecutorToFinishAll(ex);
        return ret; // ERROR
    }

    /**
     *
     * @param type
     * @return elapsed time as MILLISECONDS!
     */
    private static long doConnectedComponentsRun_Extension(CCAlgorithmType type, DBUtils db){
        STConnectedComponentsAlgo callable;
        if(NUMBER_OF_THREADS>1) {
            callable = new MTConnectedComponentsAlgo(
                    type,TimeUnit.MILLISECONDS, db);
        } else{
            // Easter Egg?!
            if(NUMBER_OF_THREADS==-1){
                NUMBER_OF_THREADS = 1;
                callable = new MTConnectedComponentsAlgo(
                        type,TimeUnit.MILLISECONDS, db);
            } else{
                callable = new STConnectedComponentsAlgo(
                        type,TimeUnit.MILLISECONDS, db);
            }
        }
        ExecutorService ex = Executors.newFixedThreadPool(1);
        long ret = -10000;
        try {
            ret = (long)(ex.submit(callable).get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        Utils.waitForExecutorToFinishAll(ex);
        return ret; // ERROR
    }


    private static long doRandomWalk(int noOfSteps, boolean extensionContext, DBUtils db){
        // INIT
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        List<Future<Long>> list = new ArrayList<>();

        for(int i=0;i<NUMBER_OF_THREADS;i++){
            MyAlgorithmBaseCallable rw;
            if(NEWSPI){
                rw = new RandomWalkKernelApiAlgorithmCallable(
                        noOfSteps/NUMBER_OF_THREADS,TimeUnit.MILLISECONDS, db);
            } else{
                rw = new RandomWalkCoreApiAlgorithmCallable(
                        noOfSteps/NUMBER_OF_THREADS,TimeUnit.MILLISECONDS, db);
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
        if(!extensionContext){
            if(NUMBER_OF_THREADS>1){
                parallelTimes_percent = parallelTimes_percent + 100;
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

        Iterator<LongCursor> it = resultCounter.keys().iterator();
        Transaction tx =db.openTransaction();
        while(it.hasNext()){
            Long l = it.next().value;
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

    private static boolean writeResultsOut(DBUtils db){
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        Stopwatch timer = Stopwatch.createUnstarted();
        int maxIndex = resultCounter.keys().size(); // something here?
        List<Future> futureList = new ArrayList<>();
        for(int i=0;i<maxIndex;i+=writeBATCHSIZE){
            NeoWriterRunnable neoWriterRunnable;
            if(i+writeBATCHSIZE>=maxIndex){
                neoWriterRunnable = new NeoWriterRunnable(PROP_ID,i,maxIndex,db);
            } else{
                neoWriterRunnable = new NeoWriterRunnable(PROP_ID,i,i+writeBATCHSIZE,db);
            }
            futureList.add(executor.submit(neoWriterRunnable));
        }
        timer.start();
        for (Future future : futureList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        timer.stop();
        boolean check = Utils.waitForExecutorToFinishAll(executor);
        System.out.println("Done Writing. (" + futureList.size() + "T) \n" + maxIndex + " Properties " +
                    "in " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms");
        return check;
    }

    private static long writeResultsOut_Extension(DBUtils db){
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        Stopwatch timer = Stopwatch.createUnstarted();
        int maxIndex = resultCounter.keys().size(); // something here?
        List<Future> futureList = new ArrayList<>();
        for(int i=0;i<maxIndex;i+=writeBATCHSIZE){
            NeoWriterRunnable neoWriterRunnable;
            if(i+writeBATCHSIZE>=maxIndex){
                neoWriterRunnable = new NeoWriterRunnable(PROP_ID,i,maxIndex,db);
            } else{
                neoWriterRunnable = new NeoWriterRunnable(PROP_ID,i,i+writeBATCHSIZE,db);
            }
            futureList.add(executor.submit(neoWriterRunnable));
        }
        timer.start();
        for (Future future : futureList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        timer.stop();
        Utils.waitForExecutorToFinishAll(executor);

        return timer.elapsed(TimeUnit.MILLISECONDS);
    }

    private static void prepaireResultMapAndCounter(DBUtils db){
        PrimitiveLongIterator it = db.getPrimitiveLongIteratorForAllNodes();
        resultCounter = new LongObjectHashMap<>((int)db.highestNodeKey);

        while(it.hasNext()){
            resultCounter.put(it.next(),new AtomicLong(0));
        }
        keySetOfResultCounter = resultCounter.keys().toArray();  // should only be called once!
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

    public static Iterator<LongCursor> getIteratorforKeySetOfResultCounter(){
        return resultCounter.keys().iterator();
    }

}
