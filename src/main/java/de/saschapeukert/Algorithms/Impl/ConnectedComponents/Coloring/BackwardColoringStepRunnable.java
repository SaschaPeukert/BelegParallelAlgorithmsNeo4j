package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.WorkerCallableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class is used in Multistep Algorithm (parallel SCC).
 * It performs a backward search on all nodes with a specific color (for a given number of all colors).
 * Those nodes found are saved as SCCs.
 * <br>
 * Created by Sascha Peukert on 20.11.2015.
 */
public class BackwardColoringStepRunnable extends WorkerCallableTemplate {

    @Override
    public void work() {

        int currentPos=startPos;

        Set<Long> allProcessedNodeIds = new HashSet<Long>();

        while(currentPos<endPos){
            long color = refArray[currentPos];

            List<Long> idList = MTConnectedComponentsAlgo.mapColorIDs.get(color);
            Set<Long> reachableIDs = BFS.go(color, Direction.INCOMING, idList); // new SCC
            allProcessedNodeIds.addAll(reachableIDs);

            MTConnectedComponentsAlgo.registerCC(reachableIDs, color);

            Iterator<Long> it = reachableIDs.iterator();
            while(it.hasNext()){
                Long o = it.next();
                MTConnectedComponentsAlgo.mapOfColors.remove(o);
            }
            currentPos++;
        }
        MTConnectedComponentsAlgo.removeFromAllNodes(allProcessedNodeIds);
    }

    public BackwardColoringStepRunnable(int startPos, int endPos, Long[] arrayOfColors ){
        super(startPos,endPos,arrayOfColors);
    }

}


