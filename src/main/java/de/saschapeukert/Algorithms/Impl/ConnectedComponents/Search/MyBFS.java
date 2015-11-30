package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.google.common.collect.Sets;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFS {

    public static volatile List<Long> frontierList= new ArrayList<>(100000);  // do not assign more than once
    public static final Set<Long> visitedIDs = Sets.newConcurrentHashSet();

    private final DBUtils db;
    private final int BATCHSIZE = 10000;

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

        frontierList.add(nodeID);
        //visitedIDs.add(nodeID);

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
                callable = new MyBFSLevelCallable(pos,frontierList.size(),frontierArray,direction,false);
            } else{
                callable = new MyBFSLevelCallable(pos,pos+BATCHSIZE,frontierArray,direction,false);
            }
            list.add(executor.submit(callable));
            pos = pos+ BATCHSIZE;
            tasks++;
        }

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
        //System.out.println("Done a level");
    }

    public void closeDownThreadPool(){
        StartComparison.waitForExecutorToFinishAll(executor);
    }
}
