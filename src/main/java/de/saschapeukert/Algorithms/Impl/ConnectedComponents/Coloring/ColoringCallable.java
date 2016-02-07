package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.WorkerCallableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Set;

/**
 * This class represents a parallel Coloring to be used in Multistep Algorithm (parallel SCC)
 * <br>
 * Created by Sascha Peukert on 04.10.2015.
 */
public class ColoringCallable extends WorkerCallableTemplate {

    private final Set<Long> privateQueue;

    @Override
    public void work() {

        int currentPos=startPos;
        while(currentPos<endPos){
            privateQueue.clear();
            long parentID = refArray[currentPos];

            Set<Long> listOfReachableNodes = expandNode(parentID,
                    MTConnectedComponentsAlgo.allNodes,false, Direction.OUTGOING);
            boolean changedAtLeastOneColor = false;

            for(Long u:listOfReachableNodes){
                if(colorIsGreaterThan(parentID,u)){
                    MTConnectedComponentsAlgo.mapOfColors.put(u, MTConnectedComponentsAlgo.mapOfColors.get(parentID));
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
            returnSet.addAll(privateQueue);

            currentPos++;
        }
    }

    public ColoringCallable(int startPos, int endPos, Long[] array){
        super(startPos,endPos,array);
        privateQueue = new HashSet<>(100000);
    }

    private boolean colorIsGreaterThan(long a, long b){
        return MTConnectedComponentsAlgo.mapOfColors.get(a) > MTConnectedComponentsAlgo.mapOfColors.get(b);
    }
}


