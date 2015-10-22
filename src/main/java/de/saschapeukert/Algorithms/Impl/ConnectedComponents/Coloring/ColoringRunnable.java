package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.MyAlgorithmBaseRunnable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class ColoringRunnable extends MyAlgorithmBaseRunnable {

    public volatile long parentID;
    private ReadOperations ops;

    public AtomicBoolean isAlive = new AtomicBoolean(true);
    public AtomicBoolean isIdle = new AtomicBoolean(true);

    public Queue<Long> resultQueue = new LinkedList<Long>();


    public ColoringRunnable( boolean output){
        super(output);
    }

    @Override
    protected void compute() {
        ops = db.getReadOperations();
        //System.out.println("Thread " + posInList + " alive");
        while (isAlive.get()) {

            //System.out.println("Thread " + posInList + " waiting");
            while(isIdle.get()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //System.out.println("Thread " + posInList + " active");
            if (!isAlive.get()) {
                //System.out.println("Thread " + posInList + " killed");
                return;
            }

            resultQueue.clear();

            while (true) {

                try {
                    parentID = MTConnectedComponentsAlgo.getElementFromQ();
                    // TODO alternative: working with offset and size than with synchronized method. maybe faster?
                    if(parentID==-1){
                        break;
                    }
                    Queue<Long> listOfReachableNodes = expandNode(parentID);

                    boolean changedAtLeastOneColor = false;
                    for(Long u:listOfReachableNodes){

                        if(colorIsGreaterThan(parentID,u)){
                            MTConnectedComponentsAlgo.mapOfColors.put(u,MTConnectedComponentsAlgo.mapOfColors.get(parentID));
                            changedAtLeastOneColor=true;
                            if(MTConnectedComponentsAlgo.mapOfVisitedNodes.get(u)==false){
                                MTConnectedComponentsAlgo.mapOfVisitedNodes.put(u, true);
                                resultQueue.add(u);
                            }
                        }


                    }

                    if(changedAtLeastOneColor){
                        if(MTConnectedComponentsAlgo.mapOfVisitedNodes.get(parentID)==false){
                            MTConnectedComponentsAlgo.mapOfVisitedNodes.put(parentID,true);
                            resultQueue.add(parentID);
                        }
                    }

                    for(Long v:resultQueue){
                        MTConnectedComponentsAlgo.mapOfVisitedNodes.put(v,false);
                    }


                } catch (IndexOutOfBoundsException e) {
                    break;
                } catch (Exception e){
                    e.printStackTrace();
                }
            }

            isIdle.set(true);
            //System.out.println("Thread " + posInList + " done");

        }
    }


    private Queue<Long> expandNode(Long id){
        Queue<Long> resultQueue = new LinkedList<Long>();

        for(Long child: db.getConnectedNodeIDs(ops, parentID, Direction.OUTGOING)){
            if(!MTConnectedComponentsAlgo.Q.contains(child)) continue;

            resultQueue.add(child);
        }
        return resultQueue;
    }

    private boolean colorIsGreaterThan(long a, long b){
        return MTConnectedComponentsAlgo.mapOfColors.get(a) > MTConnectedComponentsAlgo.mapOfColors.get(b);
    }
}


