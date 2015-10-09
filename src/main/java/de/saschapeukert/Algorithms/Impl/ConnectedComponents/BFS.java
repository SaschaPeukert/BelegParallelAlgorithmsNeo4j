package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class BFS {

    public static Queue<Long> queue;
    public static volatile List<Long> frontierList= new LinkedList<Long>();  // do not assign more than once
    public static volatile SortedMap<Integer,Queue<Long>> MapOfQueues;
    private ExecutorService executor;
    public BFS(){
        queue = new ConcurrentLinkedQueue<Long>();
        executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);
    }

    /**
     * Does the BFS
     * @param nodeID The NodeID where the search will start
     * @param direction The direction the edges will be traveled
     * @return a Set of all reachable NodeIDs
     */
    public Set<Long> go(long nodeID, Direction direction){
        ReadOperations ops =DBUtils.getReadOperations();
        Set<Long> visitedIDs = new HashSet<>();

        queue.add(nodeID);
        visitedIDs.add(nodeID);
        while(!queue.isEmpty())
        {
            Long n=queue.remove();
            if(nodeID==7){
                System.out.println("");
            }

            for(Long child: DBUtils.getConnectedNodeIDs(ops, n, direction)){
                if(visitedIDs.contains(child)){
                    continue;
                }
                visitedIDs.add(child);
                queue.add(child);

            }

        }

        return visitedIDs;
    }

    /**
     * Does the parallel BFS
     * @param nodeID The NodeID where the search will start
     * @param direction The direction the edges will be traveled
     * @return a Set of all reachable NodeIDs
     */
    public Set<Long> goParallel(long nodeID, Direction direction){
        Set<Long> visitedIDs = new HashSet<>();

        frontierList.add(nodeID);
        visitedIDs.add(nodeID);

        // start fixed Number of BFSLevelRunnable Threads
        List<BFSLevelRunnable> list = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            BFSLevelRunnable runnable = new BFSLevelRunnable(i+1,direction,  DBUtils.getGraphDb("",""),false);
            list.add(runnable);
            executor.execute(runnable);
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        MapOfQueues = new TreeMap<Integer, Queue<Long>>();
        while(!frontierList.isEmpty()){

            int size= frontierList.size();

            for(BFSLevelRunnable runnable:list){
                runnable.ignoreIDs = visitedIDs;
            }

            synchronized (frontierList){

                frontierList.notifyAll();
            }

            // awaiting results (polling)
            boolean check=false;
            while(!check){
                check = true;
                for(int i=0;i<size;i++){
                    if(MapOfQueues.get(i)==null){
                        check = false;
                    }
                }
            }

            for(BFSLevelRunnable runnable:list){
                while(!runnable.isWaiting()){};
            }

            // threads finished, collecting results
            frontierList.clear();

            for(int i=0;i<size;i++){  // build new frontier in the right order
                frontierList.addAll(MapOfQueues.get(i));
            }
            visitedIDs.addAll(frontierList);
            MapOfQueues.clear();
            System.out.println("Done 1 Step");
        }

        System.out.println("close down");

        // close down threads
        for(BFSLevelRunnable runnable:list){
            runnable.isAlive = false;
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        synchronized (frontierList){
            frontierList.notifyAll();
        }


        StartComparison.waitForExecutorToFinishAll(executor);

        return visitedIDs;
    }
}
