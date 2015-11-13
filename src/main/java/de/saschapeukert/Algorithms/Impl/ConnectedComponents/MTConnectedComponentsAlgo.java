package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.BackwardColoringStepRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring.ColoringRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.MyBFS;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */

@SuppressWarnings("deprecation")
public class MTConnectedComponentsAlgo extends STConnectedComponentsAlgo {

    private long maxdDegreeINOUT=-1;
    private long maxDegreeID=-1;
    private static Iterator<Long> itQ;   // TODO: Remove the static stuff
    private static Iterator<Long> colorIterator;

    private static boolean coloringDone;
    private static Set<Long> Q;

    public static boolean myBFS=false;
    public static  long nCutoff=100;

    public static final ConcurrentHashMap<Long, Long> mapOfColors = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, List<Long>> mapColorIDs = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, Boolean> mapOfVisitedNodes = new ConcurrentHashMap<>();

    public MTConnectedComponentsAlgo(CCAlgorithmType type, boolean output){
        super(type, output);
        if(myBFS){
            mybfs = new MyBFS();
        }
    }

    private MyBFS mybfs;

    @Override
    public void compute() {

        super.compute();

        if(myBFS){
            mybfs.closeDownThreads();
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

        MTConnectedComponentsAlgo.registerSCCandRemoveFromAllNodes(reachableIDs,componentID);

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
        FWBW_Step(myBFS); // TODO: MyBFS should be used here

        //System.out.println("Potentialy biggest component: " + componentID);
        componentID++;

        // PHASE 2
            // Start Threads
        ExecutorService executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS*2);

            // start fixed Number of ColoringRunnable Threads
        List<ColoringRunnable> listA = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        List<BackwardColoringStepRunnable> listB = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            ColoringRunnable runnable = new ColoringRunnable(false);
            BackwardColoringStepRunnable bwRunnable = new BackwardColoringStepRunnable(false);
            listA.add(runnable);
            listB.add(bwRunnable);
            executor.execute(runnable);
            executor.execute(bwRunnable);
        }

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
            Q = new HashSet<>(allNodes);
            MSColoring(listA, listB);
        }

        // PHASE 3
        //System.out.println(i+", Phase 3");
        super.strongly(); // call seq. tarjan

            // finish threads and executor
        for(WorkerRunnableTemplate runnable:listA){
            runnable.isAlive.set(false);
            runnable.isIdle.set(false);
            //System.out.println("CR: " + runnable.watch.elapsed(TimeUnit.NANOSECONDS)+ "ns");
        }
        for(WorkerRunnableTemplate runnable:listB){
            runnable.isAlive.set(false);
            runnable.isIdle.set(false);
            //System.out.println("BW: " + runnable.watch.elapsed(TimeUnit.NANOSECONDS)+ "ns");
        }

        StartComparison.waitForExecutorToFinishAll(executor);
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

    /**
     *
     * @return an element of the Q or -1 if none is available
     */
    public static synchronized long getElementFromQ(){
        if(itQ.hasNext()){
            return itQ.next();
        }
        coloringDone=true;
        return -1;
    }

    public static synchronized long getColor(){

        if(colorIterator.hasNext()){
           return colorIterator.next();
        }
        return -1;
    }

    private void MSColoring(List<ColoringRunnable> listA, List<BackwardColoringStepRunnable> listB){

        while(Q.size()!=0) {
            coloringDone = false;
            itQ = Q.iterator();

            // wake up threads
            for (ColoringRunnable cRunnable : listA) {
                cRunnable.isIdle.set(false);
            }
            // wait for threads to finish
            while (true) {

                if (coloringDone) {
                    // coloring done indicates that all nodes are given to the thread,
                    // but it dosn't mean that they finished yet!
                    boolean check = true;
                    for (ColoringRunnable runnable : listA) {
                        if (runnable.isIdle.get() == false) {
                            check = false;
                        }
                    }
                    if (check) break;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            Q.clear();
            // Barrier synchronization
            for (ColoringRunnable runnable : listA) {
                Q.addAll(runnable.resultQueue);
                runnable.resultQueue.clear();

            }
            for(Long v:Q){
                mapOfVisitedNodes.put(v,false);
            }
        }
        // Coloring done

        colorIterator = new HashSet<>(mapOfColors.values()).iterator();
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

        // start BackwardColoringStepRunnables
        for (WorkerRunnableTemplate runnable : listB) {
            runnable.isIdle.set(false);
        }

        // BFS threads work, wait for finishing
        while(true) {
            boolean check = true;
            for (BackwardColoringStepRunnable runnable : listB) {
                if (runnable.isIdle.get() == false) {
                    check = false;
                }
            }
            if (check) break;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
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

        MTConnectedComponentsAlgo.registerSCCandRemoveFromAllNodes(D,componentID);
        for(Object o:D){
            MTConnectedComponentsAlgo.mapOfColors.remove(o);
        }


    }
}

