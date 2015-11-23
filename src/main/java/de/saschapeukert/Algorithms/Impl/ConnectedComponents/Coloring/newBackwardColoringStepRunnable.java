package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.WorkerCallableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.newMTConnectedComponentsAlgo;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.Set;

/**
 * Created by Sascha Peukert on 20.11.2015.
 *
 */
public class newBackwardColoringStepRunnable extends WorkerCallableTemplate {

    @Override
    protected void compute() {

        int currentPos=startPos;

        while(currentPos<endPos){
            long color = refArray[currentPos];

            List<Long> idList = newMTConnectedComponentsAlgo.mapColorIDs.get(color);
            Set<Long> reachableIDs = BFS.go(color, Direction.INCOMING,idList); // new SCC

            newMTConnectedComponentsAlgo.registerSCCandRemoveFromAllNodes(reachableIDs, color);

            for(Object o:reachableIDs){
                MTConnectedComponentsAlgo.mapOfColors.remove(o);
            }
            currentPos++;
        }
    }

    public newBackwardColoringStepRunnable( int startPos, int endPos, Long[] arrayOfColors, boolean output ){
        super(startPos,endPos,arrayOfColors,output);
    }

}

