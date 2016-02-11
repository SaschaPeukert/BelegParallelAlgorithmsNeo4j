package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.google.common.collect.Sets;
import de.saschapeukert.Starter;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * This class represents a parallel BFS to be used in Algorithms
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFS_forkjoin {

    public Set<Long> frontierList;
    public final Set<Long> visitedIDs = Sets.newConcurrentHashSet();

    private final int BATCHSIZE = Starter.BATCHSIZE;//= 175000; // chosen by a few tests

    static Set<Long> nodeIDSet;
    private final ForkJoinPool pool;

    public MyBFS_forkjoin(){
        //executor = Executors.newFixedThreadPool(Starter.NUMBER_OF_THREADS);
        pool = new ForkJoinPool(Starter.NUMBER_OF_THREADS);
    }

    public Set<Long> work(long nodeID, Direction direction, Set<Long> set){

        if(set!=null){
            nodeIDSet = new HashSet<>(set);
        } else{
            nodeIDSet = null;
        }

        visitedIDs.clear();

        frontierList= new HashSet<>(100000);
        frontierList.add(nodeID);

        while(!frontierList.isEmpty())
        {
            // parallel
            doParallelLevel(direction);
        }
        return visitedIDs;
    }


    private void doParallelLevel(Direction direction){
        int pos=0;
        Long[] frontierArray =frontierList.toArray(new Long[frontierList.size()]);

        MyBFSLevelRecursiveTask task = new MyBFSLevelRecursiveTask(pos,frontierList.size(),frontierArray,direction);
        ForkJoinTask t = pool.submit(task);

        visitedIDs.addAll(frontierList);
        frontierList.clear();
        // threads finished, collecting results -> new frontier

        try {
            List<Long> completeList = (List<Long>) t.get();
            frontierList.addAll(completeList);
            frontierList.removeAll(visitedIDs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if(MyBFS_forkjoin.nodeIDSet!=null){
            // this is a backward sweep
            frontierList.retainAll(nodeIDSet);
        }
    }

}
