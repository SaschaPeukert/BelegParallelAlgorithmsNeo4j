package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.google.common.collect.Sets;
import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class MyBFS {

    public static volatile List<Long> frontierList= new LinkedList<>();  // do not assign more than once
    public static volatile SortedMap<Integer,Queue<Long>> MapOfQueues;
    private static volatile List<Boolean> ThreadCheckList = new LinkedList<>();
    public static Set<Long> visitedIDs = Sets.newConcurrentHashSet();

    private List<MyBFSLevelRunnable> list;
    private final int THRESHOLD=100000;
    private final DBUtils db;

    private final ExecutorService executor;

    private static MyBFS instance;

    public static MyBFS getInstance(){
        if(instance==null){
            instance = new MyBFS();
        }
        return instance;
    }

    private MyBFS(){
        executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);

        // start fixed Number of MyBFSLevelRunnable Threads
        list = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            MyBFSLevelRunnable runnable = new MyBFSLevelRunnable(i,false);
            list.add(runnable);
            executor.execute(runnable);
        }

        MapOfQueues = new ConcurrentSkipListMap<>();
        db = DBUtils.getInstance("","");
    }


    public Set<Long> work(long nodeID, Direction direction){

        ReadOperations ops = db.getReadOperations();
        //Set<Long> visitedIDs = new HashSet<>();
        visitedIDs.clear();

        frontierList.add(nodeID);

        visitedIDs.add(nodeID);
        while(!frontierList.isEmpty())
        {
            int size = frontierList.size();
            if(size>THRESHOLD){
                // parallel
                doParallelLevel(direction,size);
            } else{
                // sequential
                doSequentialLevel(direction,ops);

            }

        }
        return visitedIDs;
    }

    private void doSequentialLevel(Direction direction, ReadOperations ops){
        Long n = frontierList.remove(0);

        for(Long child: db.getConnectedNodeIDs(ops, n, direction)) {
            if (visitedIDs.contains(child)) {
                continue;
            }
            visitedIDs.add(child);
            frontierList.add(child);
        }

    }

    private void doParallelLevel(Direction direction, int size){

        ThreadCheckList.clear();
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            ThreadCheckList.add(false);
        }

        for(MyBFSLevelRunnable runnable:list){
            runnable.direction = direction;
            runnable.isIdle.set(false);
        }

        // checking if results available
        boolean check;
        while(true){
            //System.out.println(MapOfQueues.keySet().size() + "/" + size);

            check=true;
            for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
                if(!checkThreadList(i)){
                    check=false;
                }
            }
            if(!check){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else{
                break;
            }

        }

        // threads finished, collecting results -> new frontier
        frontierList.clear();
        for(int i=0;i<size;i++){

            //System.out.println(i +" s:" + size);
            frontierList.addAll(MapOfQueues.get(i));
        }

        MapOfQueues.clear();
        //System.out.println("Done a level");
    }

    public void closeDownThreads(){

        for(WorkerRunnableTemplate runnable:list){
            runnable.isAlive.set(false);
            runnable.isIdle.set(false);
        }

        StartComparison.waitForExecutorToFinishAll(executor);
    }

    private synchronized boolean checkThreadList(int i){
        return ThreadCheckList.get(i);
    }

    public static synchronized void setCheckThreadList(int i){
        ThreadCheckList.set(i,true);
    }
}
