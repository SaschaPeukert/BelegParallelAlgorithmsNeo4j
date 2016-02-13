package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Starter;
import de.saschapeukert.Utils;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This class represents a parallel BFS to be used in Algorithms
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFS {

    public HashSet<Long> frontierList;
    public final HashSet<Long> visitedIDs = new HashSet<>(10000);//Sets.newConcurrentHashSet();

    private final int BATCHSIZE = Starter.BATCHSIZE;//= 175000; // chosen by a few tests

    static HashSet<Long> nodeIDSet;
    private final ExecutorService executor;

    public MyBFS(){
        executor = Executors.newFixedThreadPool(Starter.NUMBER_OF_THREADS);

    }

    public HashSet<Long> work(long nodeID, Direction direction, Set<Long> set){

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
        int tasks =0;
        Long[] frontierArray = frontierList.toArray(new Long[frontierList.size()]);
        List<Future<List<Long>>> list = new ArrayList<>();
        while(pos<frontierList.size()){
            MyBFSLevelCallable callable;
            if((pos+BATCHSIZE)>=frontierList.size()){
                callable = new MyBFSLevelCallable(pos,frontierList.size(),frontierArray,direction);
            } else{
                callable = new MyBFSLevelCallable(pos,pos+BATCHSIZE,frontierArray,direction);
            }
            list.add(executor.submit(callable));
            pos = pos+ BATCHSIZE;
            tasks++;
        }
        visitedIDs.addAll(frontierList);
        frontierList.clear();
        // threads finished, collecting results -> new frontier
        for(int i=0;i<tasks;i++) {
            try {
                List<Long> partOfList = list.get(i).get();
                //frontierList.removeAll(partOfList);

                Iterator<Long> it = partOfList.iterator();
                while(it.hasNext()){
                    Long lo = it.next();
                    frontierList.add(lo);
                }
                //frontierList.addAll(partOfList);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        frontierList.removeAll(visitedIDs);

        if(MyBFS.nodeIDSet!=null){
            // this is a backward sweep
            frontierList.retainAll(nodeIDSet);
        }
    }

    public void closeDownThreadPool(){
        Utils.waitForExecutorToFinishAll(executor);
    }
}
