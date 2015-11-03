package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Coloring;

import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.MTConnectedComponentsAlgo;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 *
 *  They will stop themself after the can't get new colors
 */
public class BackwardColoringStepRunnable extends WorkerRunnableTemplate {

    public BackwardColoringStepRunnable(boolean output ){
        super(output);
    }

    @Override
    protected boolean operation(){

        long color = MTConnectedComponentsAlgo.getColor();
        if(color ==-1){
            return false;  // exit loop
        }

        List<Long> idList = MTConnectedComponentsAlgo.mapColorIDs.get(color);
        Set<Long> reachableIDs = BFS.go(color, Direction.INCOMING,idList); // new SCC

        MTConnectedComponentsAlgo.registerSCCandRemoveFromAllNodes(reachableIDs,(int) color);

        for(Object o:reachableIDs){
            MTConnectedComponentsAlgo.mapOfColors.remove(o);
        }
        return true; // loop !
    }

    //@Override
    //protected void cleanUpOperation(){
    //    isAlive.set(false);  // stop yourself
    //}

}


