package de.saschapeukert.algorithms.impl.connected.components.coloring;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import de.saschapeukert.algorithms.abst.WorkerCallableTemplate;
import de.saschapeukert.algorithms.impl.connected.components.MTConnectedComponentsAlgo;
import de.saschapeukert.algorithms.impl.connected.components.search.BFS;
import de.saschapeukert.database.DBUtils;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

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
        LongHashSet allProcessedIds = new LongHashSet();

        while(currentPos<endPos){
            long color = refArray[currentPos];

            LongArrayList idList = MTConnectedComponentsAlgo.mapColorIDs.get(color);
            LongHashSet reachableIDs = BFS.go(color, Direction.INCOMING, idList, db); // new SCC
            allProcessedIds.addAll(reachableIDs);
            MTConnectedComponentsAlgo.registerCC(reachableIDs, color);

            Iterator<LongCursor> it =reachableIDs.iterator();
            while(it.hasNext()){
                Long o = it.next().value;
                MTConnectedComponentsAlgo.mapOfColors.remove(o);
            }
            /*for(Object o:reachableIDs){
                MTConnectedComponentsAlgo.mapOfColors.remove(o);
            }*/
            currentPos++;
        }
        MTConnectedComponentsAlgo.removeFromAllNodes(allProcessedIds);
    }

    public BackwardColoringStepRunnable(int startPos, int endPos, long[] arrayOfColors, DBUtils db){
        super(startPos,endPos,arrayOfColors, db);
    }

}


