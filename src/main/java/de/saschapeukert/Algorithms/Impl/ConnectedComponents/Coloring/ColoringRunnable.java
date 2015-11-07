package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class ColoringRunnable extends WorkerRunnableTemplate {

    public final Set<Long> resultQueue;
    private final Set<Long> privateQueue;

    public ColoringRunnable( boolean output){
        super(output);
        resultQueue = new HashSet<>(100000);
        privateQueue = new HashSet<>(100000);
    }

    @Override
    protected boolean operation(){

        privateQueue.clear();
        parentID = MTConnectedComponentsAlgo.getElementFromQ();

        if(parentID==-1){
            return false;
        }

        Set<Long> listOfReachableNodes = expandNode(parentID,
                MTConnectedComponentsAlgo.allNodes,false, Direction.OUTGOING);
        boolean changedAtLeastOneColor = false;

        for(Long u:listOfReachableNodes){
            if(colorIsGreaterThan(parentID,u)){
                MTConnectedComponentsAlgo.mapOfColors.put(u,MTConnectedComponentsAlgo.mapOfColors.get(parentID));
                changedAtLeastOneColor=true;
                if(MTConnectedComponentsAlgo.mapOfVisitedNodes.get(u)==false){
                    MTConnectedComponentsAlgo.mapOfVisitedNodes.put(u, true);
                    privateQueue.add(u);
                }
            }
        }

        if(changedAtLeastOneColor){
            if(MTConnectedComponentsAlgo.mapOfVisitedNodes.get(parentID)==false){
                MTConnectedComponentsAlgo.mapOfVisitedNodes.put(parentID,true);
                privateQueue.add(parentID);
            }
        }
        resultQueue.addAll(privateQueue);
        return true;
    }

    private boolean colorIsGreaterThan(long a, long b){
        return MTConnectedComponentsAlgo.mapOfColors.get(a) > MTConnectedComponentsAlgo.mapOfColors.get(b);
    }
}


