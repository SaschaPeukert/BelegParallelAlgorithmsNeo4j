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
    public static final List<Long> frontierList= new LinkedList<Long>();  // do not assign more than once
    public static SortedMap<Integer,Queue<Long>> MapOfQueues;
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

        List<BFSLevelRunnable> list = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            BFSLevelRunnable runnable = new BFSLevelRunnable(direction,  DBUtils.getGraphDb("",""),false);
            list.add(runnable);
            executor.execute(runnable);
        }


        while(!queue.isEmpty())
        {
            //List<BFSLevelRunnable> list = new ArrayList<>();
            int t=0;
            while(!frontierList.isEmpty()){
                Long n=frontierList.remove(0);
                n.wait();

                BFSLevelRunnable workOne = list.get(t);
                while (!workOne.hasFinished){
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                workOne.parentID=
                //BFSLevelRunnable runnable = new BFSLevelRunnable(n,direction, visitedIDs, DBUtils.getGraphDb("",""),false);
                //list.add(runnable);
                //executor.execute(runnable);

            }
            StartComparison.waitForExecutorToFinishAll(executor);
            executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);

            // Synchronize on level basis
            for(BFSLevelRunnable runnable:list){
                Queue<Long> q = runnable.getQueue();
                frontierList.addAll(q);

                visitedIDs.addAll(q);
                runnable = null;
            }

        }

        return visitedIDs;
    }
}
