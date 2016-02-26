package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Stopwatch;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.BackwardColoringStepRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.ColoringCallable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.MyBFS;
import de.saschapeukert.Starter;
import de.saschapeukert.Utils;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * This class represents the multi thread Strongly Connected Components Algorithm (Multistep) using the Kernel API of Neo4j
 * <br>
 * Created by Sascha Peukert on 06.08.2015.
 */

@SuppressWarnings("deprecation")
public class MTConnectedComponentsAlgo extends STConnectedComponentsAlgo {

    private long maxdDegreeINOUT=-1;
    private long maxDegreeID=-1;

    public static boolean myBFS=false;
    public static  long nCutoff=1000; // TODO test this

    public static final ConcurrentHashMap<Long, Long> mapOfColors = new ConcurrentHashMap<>();
    public static final LongObjectHashMap<LongArrayList> mapColorIDs = new LongObjectHashMap<>();
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
        LongHashSet reachableIDs;
        if(myBFS) {
            reachableIDs = mybfs.work(n, Direction.BOTH, null);
            parallelTime = parallelTime + mybfs.parallelTimer.elapsed(timeUnit);
        } else{
            reachableIDs = BFS.go(n,Direction.BOTH);
        }
        registerCC(reachableIDs,componentID);
        removeFromAllNodes(reachableIDs);
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
        ExecutorService executor = Executors.newFixedThreadPool(Starter.NUMBER_OF_THREADS);
        //System.out.println("Phase 2");
        int i=0;
        while(nCutoff<allNodes.size()) { // Do MS-Coloring
            i++;
            if(i!=1){
                mapOfColors.clear();
                Iterator<LongCursor> it = allNodes.iterator();
                while(it.hasNext()){
                    Long lo = it.next().value;
                    mapOfColors.put(lo,lo);
                }
            }
            LongHashSet Q = new LongHashSet(allNodes);
            //Set<Long> Q = new HashSet<>(allNodes);
            MSColoring(executor, Q);
        }

        // PHASE 3
        //System.out.println(i+", Phase 3");
        super.strongly(); // call seq. tarjan

            // finish threads and executor
        Utils.waitForExecutorToFinishAll(executor);
    }

    @Override
    protected void furtherInspectNodeWhileTrim(Long n){  //<- FIXME
        if(myType==CCAlgorithmType.STRONG){
            long degreeINOUT = db.getDegree(n,Direction.INCOMING)*db.getDegree(n,Direction.OUTGOING);
            if(degreeINOUT>maxdDegreeINOUT){
                maxdDegreeINOUT =degreeINOUT;
                maxDegreeID = n;
            }
            mapOfColors.put(n, n);
            mapOfVisitedNodes.put(n,false);
        }
    }

    private void MSColoring(ExecutorService executor, LongHashSet Q){
        int tasks;
        int pos;
        Stopwatch pTimer = Stopwatch.createUnstarted();

        int cBATCHSIZE = Starter.BATCHSIZE*15;
        while(Q.size()!=0) {

            // wake up threads
            tasks=0;
            pos=0;
            long[] queueArray = Q.toArray();//toArray(new long[Q.size()]);
            List<Future<LongHashSet>> list = new ArrayList<>();
            while(pos<Q.size()){
                ColoringCallable callable;
                if((pos+ cBATCHSIZE)>=Q.size()){
                    callable = new ColoringCallable(pos,Q.size(),queueArray);
                } else{
                    callable = new ColoringCallable(pos,pos+ cBATCHSIZE,queueArray);
                }
                list.add(executor.submit(callable));
                pos = pos+ cBATCHSIZE; // new startPos = old EndPos
                tasks++;
            }
            pTimer.start();
            Q.clear();
            // Barrier synchronization
            for(int i=0;i<tasks;i++){
                try {
                    Q.addAll(list.get(i).get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            pTimer.stop();
            parallelTime = parallelTime + pTimer.elapsed(timeUnit);
            pTimer.reset();
            Iterator<LongCursor> it = Q.iterator();
            while(it.hasNext()){
                Long v = it.next().value;
                mapOfVisitedNodes.put(v,false);
            }
        }
        // Coloring done

        // prepare mapColorIDs
        for(Long id:mapOfColors.keySet()){
            long color = mapOfColors.get(id);
            if(mapColorIDs.containsKey(color)){
                LongArrayList li = mapColorIDs.get(color);
                li.add(id);
            } else{
                LongArrayList l = new LongArrayList();
                l.add(id);
                mapColorIDs.put(color,l);
            }
        }

        // start BackwardColoringStepCallables

        pos=0;
        tasks=0;
        List<Future<Set<Long>>> list = new ArrayList<>();
        long[] colorArray = mapColorIDs.keys().toArray();
                //mapColorIDs.keySet().toArray(new Long[mapColorIDs.keySet().size()]);
        while(pos<mapColorIDs.keys().size()){
            BackwardColoringStepRunnable callable;
            if((pos+ cBATCHSIZE)>=Q.size()){
                callable = new BackwardColoringStepRunnable(pos,mapColorIDs.keys().size(),colorArray);
            } else{
                callable = new BackwardColoringStepRunnable(pos,pos+ cBATCHSIZE,colorArray);
            }
            list.add(executor.submit(callable));
            pos = pos+ cBATCHSIZE; // new startPos = old EndPos
            tasks++;
        }
        pTimer.start();
        // BFS threads work, wait for finishing
        for(int i=0;i<tasks;i++){
            try {
               list.get(i).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        pTimer.stop();
        parallelTime = parallelTime + pTimer.elapsed(timeUnit);
    }

    private void FWBW_Step(boolean myBFS){
        LongHashSet D;
        if(myBFS){
            D = mybfs.work(maxDegreeID, Direction.OUTGOING,null);
            parallelTime = parallelTime + mybfs.parallelTimer.elapsed(timeUnit);
            //System.out.println(D.size());
            D.retainAll(mybfs.work(maxDegreeID, Direction.INCOMING, D)); // D = S from Paper from here on

        } else{
            D = BFS.go(maxDegreeID, Direction.OUTGOING);
            D.retainAll(BFS.go(maxDegreeID, Direction.INCOMING, D)); // D = S from Paper from here on
        }

        registerCC(D,componentID);
        removeFromAllNodes(D);
        Iterator<LongCursor> it =D.iterator();
        while(it.hasNext()){
            Long o = it.next().value;
            mapOfColors.remove(o);
        }

    }
}

