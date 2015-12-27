package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.BackwardColoringStepRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.ColoringCallable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.MyBFS;
import de.saschapeukert.Utils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */

@SuppressWarnings("deprecation")
public class MTConnectedComponentsAlgo extends STConnectedComponentsAlgo {

    private long maxdDegreeINOUT=-1;
    private long maxDegreeID=-1;

    public static boolean myBFS=true;
    public static  long nCutoff=10000; // TODO test this
    private int BATCHSIZE = 10000;  // TODO test this

    public static final ConcurrentHashMap<Long, Long> mapOfColors = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, List<Long>> mapColorIDs = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, Boolean> mapOfVisitedNodes = new ConcurrentHashMap<>();

    public MTConnectedComponentsAlgo(CCAlgorithmType type, TimeUnit tu){
        super(type, tu);
        if(myBFS){
            mybfs = new MyBFS();
        }
    }

    private MyBFS mybfs;

    @Override
    public void work() {

        super.work();

        if(myBFS){
            mybfs.closeDownThreadPool();
        }
    }


    @Override
    protected void searchForWeakly(long n){
        Set<Long> reachableIDs;
        if(myBFS) {
            reachableIDs = mybfs.work(n, Direction.BOTH, null);
        } else{
            reachableIDs = BFS.go(n,Direction.BOTH);
        }
        registerSCCandRemoveFromAllNodes(reachableIDs,componentID);
    }

    /**
     *  Using the <b>Multistep Algorithm</b> described in the Paper  "BFS and Coloring-based Parallel Algorithms for
     *  Strongly Connected Components and Related Problems":
     *  <br><br>
     *  http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=6877288
     */
    @Override
    protected void strongly(){
        // PHASE 1
        FWBW_Step(myBFS);

        //System.out.println("Potentialy biggest component: " + componentID);
        componentID++;

        // PHASE 2
            // Start Threads
        ExecutorService executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);
        //System.out.println("Phase 2");
        int i=0;
        while(nCutoff<allNodes.size()) { // Do MS-Coloring
            i++;
            if(i!=1){
                mapOfColors.clear();
                for(Long lo:allNodes){
                    mapOfColors.put(lo,lo);
                }
            }
            Set<Long> Q = new HashSet<>(allNodes);
            MSColoring(executor, Q);
        }

        // PHASE 3
        //System.out.println(i+", Phase 3");
        super.strongly(); // call seq. tarjan

            // finish threads and executor
        Utils.waitForExecutorToFinishAll(executor);
    }

    @Override
    protected void furtherInspectNodeWhileTrim(Node n){
        if(myType==CCAlgorithmType.STRONG){
            long degreeINOUT = n.getDegree(Direction.INCOMING)*n.getDegree(Direction.OUTGOING);
            if(degreeINOUT>maxdDegreeINOUT){
                maxdDegreeINOUT =degreeINOUT;
                maxDegreeID = n.getId();
            }
            mapOfColors.put(n.getId(), n.getId());
            mapOfVisitedNodes.put(n.getId(),false);
        }
    }

    private void MSColoring(ExecutorService executor, Set<Long> Q){
        int tasks;
        int pos;

        while(Q.size()!=0) {

            // wake up threads
            tasks=0;
            pos=0;
            Long[] queueArray = Q.toArray(new Long[Q.size()]);
            List<Future<Set<Long>>> list = new ArrayList<>();
            while(pos<Q.size()){
                ColoringCallable callable;
                if((pos+BATCHSIZE)>=Q.size()){
                    callable = new ColoringCallable(pos,Q.size(),queueArray);
                } else{
                    callable = new ColoringCallable(pos,pos+BATCHSIZE,queueArray);
                }
                list.add(executor.submit(callable));
                pos = pos+ BATCHSIZE; // new startPos = old EndPos
                tasks++;
            }

            Q.clear();
            // Barrier synchronization
            for(int i=0;i<tasks;i++){
                try {
                    Q.addAll(list.get(i).get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            for(Long v:Q){
                mapOfVisitedNodes.put(v,false);
            }
        }
        // Coloring done

        // prepare mapColorIDs
        for(Long id:mapOfColors.keySet()){
            long color = mapOfColors.get(id);
            if(mapColorIDs.containsKey(color)){
                List<Long> li = mapColorIDs.get(color);
                li.add(id);
            } else{
                List<Long> l = new ArrayList<>();
                l.add(id);
                mapColorIDs.put(color,l);
            }
        }

        // start BackwardColoringStepCallables
        pos=0;
        tasks=0;
        List<Future<Set<Long>>> list = new ArrayList<>();
        Long[] colorArray = mapColorIDs.keySet().toArray(new Long[mapColorIDs.keySet().size()]);
        while(pos<mapColorIDs.keySet().size()){
            BackwardColoringStepRunnable callable;
            if((pos+BATCHSIZE)>=Q.size()){
                callable = new BackwardColoringStepRunnable(pos,mapColorIDs.keySet().size(),colorArray);
            } else{
                callable = new BackwardColoringStepRunnable(pos,pos+BATCHSIZE,colorArray);
            }
            list.add(executor.submit(callable));
            pos = pos+ BATCHSIZE; // new startPos = old EndPos
            tasks++;
        }
        // BFS threads work, wait for finishing
        for(int i=0;i<tasks;i++){
            try {
               list.get(i).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private void FWBW_Step(boolean myBFS){
        Set<Long> D;
        if(myBFS){
            D = mybfs.work(maxDegreeID, Direction.OUTGOING,null);
            //System.out.println(D.size());
            D.retainAll(mybfs.work(maxDegreeID, Direction.INCOMING, D)); // D = S from Paper from here on

        } else{
            D = BFS.go(maxDegreeID, Direction.OUTGOING);
            D.retainAll(BFS.go(maxDegreeID, Direction.INCOMING, D)); // D = S from Paper from here on
        }

        registerSCCandRemoveFromAllNodes(D,componentID);
        for(Object o:D){
            mapOfColors.remove(o);
        }


    }
}

