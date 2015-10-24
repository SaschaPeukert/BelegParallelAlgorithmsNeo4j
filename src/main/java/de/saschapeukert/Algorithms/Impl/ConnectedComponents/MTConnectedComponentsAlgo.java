package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

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
    public static Set<Long> Q;

    private static final long nCutoff=100000;

    public static final ConcurrentHashMap<Long, Long> mapOfColors = new ConcurrentHashMap<>();
    public static final HashMap<Long, List<Long>> mapColorIDs = new HashMap<>();
    public static final ConcurrentHashMap<Long, Boolean> mapOfVisitedNodes = new ConcurrentHashMap<>();

    public MTConnectedComponentsAlgo(CCAlgorithmType type, boolean output){
        super(type, output);
    }


    @Override
    public void compute() {

        super.compute();

        if(myType==CCAlgorithmType.WEAK){
            MyBFS.getInstance().closeDownThreads();
        }
    }


    @Override
    protected void searchForWeakly(long n){

        MyBFS bfs = MyBFS.getInstance();
        Set<Long> reachableIDs = bfs.work(n, Direction.BOTH);

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
            // NOT YET IMPLEMENTED
        //throw new NotImplementedException();
            // TODO: check/fix what happens if maxDegreeID is still on default value

        // PHASE 1

            // TODO: MyBFS should be used here
        Set<Long> D = BFS.go(maxDegreeID, Direction.OUTGOING);
        D.retainAll(BFS.go(maxDegreeID, Direction.INCOMING, D)); // D = S from Paper from here on

        MTConnectedComponentsAlgo.registerSCCandRemoveFromAllNodes(D,componentID);

        componentID++;

            // TODO: Don't forget to close down threads

        // PHASE 2

        Q = new HashSet<>(allNodes);

            // Start Threads

        ExecutorService executor = Executors.newFixedThreadPool(StartComparison.NUMBER_OF_THREADS);

            // start fixed Number of ColoringRunnable Threads
        ArrayList<ColoringRunnable> list = new ArrayList<>(StartComparison.NUMBER_OF_THREADS);
        for(int i=0;i<StartComparison.NUMBER_OF_THREADS;i++){
            ColoringRunnable runnable = new ColoringRunnable(false);
            list.add(runnable);
            executor.execute(runnable);
        }

        while(Q.size()!=0) {

            coloringDone = false;
            itQ = Q.iterator();

            // wake up threads
            for (ColoringRunnable cRunnable : list) {
                cRunnable.isIdle.set(false);
            }
            // wait
            while (true) {

                if (coloringDone) {
                    // coloring done indicates that all nodes are given to the thread,
                    // but it dosn't mean that they finished yet!

                    boolean check = true;
                    for (ColoringRunnable runnable : list) {
                        if (runnable.isIdle.get() == false) {
                            check = false;
                        }
                    }
                    if (check) break;
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            Q.clear();
            // Barrier synchronization
            for (ColoringRunnable runnable : list) {
                Q.addAll(runnable.resultQueue);
            }

        }   // is this right?!?
        colorIterator = new HashSet<>(mapOfColors.values()).iterator();

        // prepare mapColorIDs
        for(Long id:mapOfColors.keySet()){
            long color = mapOfColors.get(id);
            if(mapColorIDs.containsKey(color)){
                mapColorIDs.get(color).add(id);
            } else{
                List l = new ArrayList();
                l.add(id);
                mapColorIDs.put(id,l);
            }
        }

        // start BackwardColoringStepRunnables

        // BFS threads work


        // PHASE 3

        super.strongly(); // call seq. tarjan


        // finish executors
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
        if(!(nCutoff>=allNodes.size())){
            if(colorIterator.hasNext()){
                return colorIterator.next();
            }
        }
        return -1;
    }
}

