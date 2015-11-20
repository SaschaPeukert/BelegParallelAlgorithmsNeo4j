package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.google.common.collect.Sets;
import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Sascha Peukert on 19.11.2015.
 */
public class newMyBFS {

    public static volatile List<Long> frontierList= new ArrayList<>(100000);  // do not assign more than once
    public static final Set<Long> visitedIDs = Sets.newConcurrentHashSet();

    private static boolean levelDone;
    private static Iterator<Long> it;

    private final List<MyBFSLevelRunnable> list;
    private final int THRESHOLD=0;
    private final DBUtils db;

    static Set<Long> nodeIDSet;
    private final ExecutorService executor;

    public newMyBFS(){
        executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);

        // start fixed Number of MyBFSLevelRunnable Threads
        list = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            MyBFSLevelRunnable runnable = new MyBFSLevelRunnable(i,false);
            list.add(runnable);
            executor.execute(runnable);
        }
        //MapOfQueues = new ConcurrentSkipListMap<>();
        db = DBUtils.getInstance("","");
    }


    public Set<Long> work(long nodeID, Direction direction, Set<Long> set){

        if(set!=null){
            nodeIDSet = new HashSet<>(set);
        } else{
            nodeIDSet = null;
        }

        ReadOperations ops = db.getReadOperations();
        //Set<Long> visitedIDs = new HashSet<>();
        visitedIDs.clear();

        frontierList.add(nodeID);
        visitedIDs.add(nodeID);

        while(!frontierList.isEmpty())
        {
            if(frontierList.size()>THRESHOLD){
                // parallel
                doParallelLevel(direction);
            } else{
                // sequential
                doSequentialLevel(direction,ops);
            }
        }
        return visitedIDs;
    }

    private void doSequentialLevel(Direction direction, ReadOperations ops){
        Long n = frontierList.remove(0);

        visitedIDs.add(n);
        Set<Long> resultQueue = new HashSet<>(db.getConnectedNodeIDs(ops, n, direction));
        if(nodeIDSet!=null){
            resultQueue.retainAll(newMyBFS.nodeIDSet);
        }
        resultQueue.removeAll(visitedIDs);

        frontierList.addAll(resultQueue);
    }

    private void doParallelLevel(Direction direction){
        levelDone = false;
        it = frontierList.iterator();
        for(MyBFSLevelRunnable runnable:list){
            runnable.direction = direction;
            runnable.isIdle.set(false);
        }

        // checking if results available
        boolean check;
        while(true){
            //System.out.println(MapOfQueues.keySet().size() + "/" + size);
            if(levelDone){
                check=true;
                for (MyBFSLevelRunnable runnable : list) {
                    if (runnable.isIdle.get() == false) {
                        check = false;
                    }
                }
                if(check){
                    break;
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // threads finished, collecting results -> new frontier
        frontierList.clear();

        for(MyBFSLevelRunnable runnable : list){
            frontierList.addAll(runnable.resultQueue);
            runnable.resultQueue.clear();
        }
        //System.out.println("Done a level");
    }

    public void closeDownThreads(){

        for(WorkerRunnableTemplate runnable:list){
            runnable.isAlive.set(false);
            runnable.isIdle.set(false);
        }
        StartComparison.waitForExecutorToFinishAll(executor);
    }

    public static synchronized long getKey(){
        if(it.hasNext()){
            return it.next();
        }
        levelDone=true;
        return -1;
    }

    public ExecutorService getExecutor(){
        return executor;
    }
}
