package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.BackwardColoringStepRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.ColoringCallable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.MyBFS;
import de.saschapeukert.Starter;
import de.saschapeukert.Utils;
import org.neo4j.graphdb.Direction;

import java.util.*;
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

    public static boolean myBFS=true;
    public static  long nCutoff=1000; // TODO test this

    public static final ConcurrentHashMap<Long, Long> mapOfColors = new ConcurrentHashMap<>();
    public static final Map<Long,List<Long>> mapColorIDs = new HashMap<>();
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
                Iterator<Long> it = allNodes.iterator();
                while(it.hasNext()){
                    Long lo = it.next();
                    mapOfColors.put(lo,lo);
                }
            }
            HashSet<Long> Q = new HashSet<>(allNodes);
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

    private void MSColoring(ExecutorService executor, Set<Long> Q){
        int tasks;
        int pos;

        int sBATCHSIZE = 10000;
        while(Q.size()!=0) {

            // wake up threads
            tasks=0;
            pos=0;
            Long[] queueArray = Q.toArray(new Long[Q.size()]);
            List<Future<HashSet<Long>>> list = new ArrayList<>();
            while(pos<Q.size()){
                ColoringCallable callable;
                if((pos+ sBATCHSIZE)>=Q.size()){
                    callable = new ColoringCallable(pos,Q.size(),queueArray);
                } else{
                    callable = new ColoringCallable(pos,pos+ sBATCHSIZE,queueArray);
                }
                list.add(executor.submit(callable));
                pos = pos+ sBATCHSIZE; // new startPos = old EndPos
                tasks++;
            }

            Q.clear();
            // Barrier synchronization
            for(int i=0;i<tasks;i++){
                try {
                    Q.addAll(list.get(i).get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            Iterator<Long> it = Q.iterator();
            while(it.hasNext()){
                Long v = it.next();
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
                ArrayList<Long> l = new ArrayList<>();
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
            if((pos+ sBATCHSIZE)>=Q.size()){
                callable = new BackwardColoringStepRunnable(pos,mapColorIDs.keySet().size(),colorArray);
            } else{
                callable = new BackwardColoringStepRunnable(pos,pos+ sBATCHSIZE,colorArray);
            }
            list.add(executor.submit(callable));
            pos = pos+ sBATCHSIZE; // new startPos = old EndPos
            tasks++;
        }
        // BFS threads work, wait for finishing
        for(int i=0;i<tasks;i++){
            try {
               list.get(i).get();
            } catch (InterruptedException | ExecutionException e) {
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

        registerCC(D,componentID);
        removeFromAllNodes(D);
        Iterator<Long> it =D.iterator();
        while(it.hasNext()){
            Long o = it.next();
            mapOfColors.remove(o);
        }

    }
}

