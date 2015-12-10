package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.google.common.collect.Sets;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Utils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFS {

    public List<Long> frontierList;
    public final Set<Long> visitedIDs = Sets.newConcurrentHashSet();

    private final DBUtils db;
    private final int BATCHSIZE = 200000; // chosen by a few tests

    static Set<Long> nodeIDSet;
    private final ExecutorService executor;

    public MyBFS(){
        executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);
        db = DBUtils.getInstance("","");
    }

    public Set<Long> work(long nodeID, Direction direction, Set<Long> set){

        if(set!=null){
            nodeIDSet = new HashSet<>(set);
        } else{
            nodeIDSet = null;
        }

        visitedIDs.clear();

        frontierList= new ArrayList<>(100000);
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
        Long[] frontierArray =frontierList.toArray(new Long[frontierList.size()]);
        List<Future<Set<Long>>> list = new ArrayList<>();
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
        for(int i=0;i<tasks;i++){
            try {
                frontierList.addAll(list.get(i).get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        list = null;
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
