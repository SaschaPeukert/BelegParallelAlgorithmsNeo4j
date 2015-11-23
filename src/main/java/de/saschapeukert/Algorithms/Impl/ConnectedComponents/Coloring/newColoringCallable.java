package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.WorkerCallableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.newMTConnectedComponentsAlgo;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class newColoringCallable extends WorkerCallableTemplate {

    private final Set<Long> privateQueue;

    @Override
    protected void compute() {

        int currentPos=startPos;
        while(currentPos<endPos){
            privateQueue.clear();
            long parentID = refArray[currentPos];

            Set<Long> listOfReachableNodes = expandNode(parentID,
                    newMTConnectedComponentsAlgo.allNodes,false, Direction.OUTGOING);
            boolean changedAtLeastOneColor = false;

            for(Long u:listOfReachableNodes){
                if(colorIsGreaterThan(parentID,u)){
                    newMTConnectedComponentsAlgo.mapOfColors.put(u,newMTConnectedComponentsAlgo.mapOfColors.get(parentID));
                    changedAtLeastOneColor=true;
                    if(newMTConnectedComponentsAlgo.mapOfVisitedNodes.get(u)==false){
                        newMTConnectedComponentsAlgo.mapOfVisitedNodes.put(u, true);
                        privateQueue.add(u);
                    }
                }
            }

            if(changedAtLeastOneColor){
                if(newMTConnectedComponentsAlgo.mapOfVisitedNodes.get(parentID)==false){
                    newMTConnectedComponentsAlgo.mapOfVisitedNodes.put(parentID,true);
                    privateQueue.add(parentID);
                }
            }
            returnSet.addAll(privateQueue);

            currentPos++;
        }
    }

    public newColoringCallable(int startPos, int endPos, Long[] array, boolean output){
        super(startPos,endPos,array,output);
        privateQueue = new HashSet<>(100000);
    }

    private boolean colorIsGreaterThan(long a, long b){
        return newMTConnectedComponentsAlgo.mapOfColors.get(a) > newMTConnectedComponentsAlgo.mapOfColors.get(b);
    }
}


