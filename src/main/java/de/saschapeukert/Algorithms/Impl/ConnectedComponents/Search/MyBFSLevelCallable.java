package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Algorithms.Abst.WorkerCallableTemplate;
import org.neo4j.graphdb.Direction;

import java.util.List;

/**
 * This class is needed by MyBFS to explore a given part of the frontier in parallel.
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFSLevelCallable extends WorkerCallableTemplate {

    public Direction direction;

    public MyBFSLevelCallable(int startPos, int endPos, Long[] array, Direction direction){
        super(startPos,endPos,array);
        this.direction = direction;
    }

    @Override
    public void work() {

        int currentPos=startPos;
        while(currentPos<endPos){
            long parentID = refArray[currentPos];
            //Set<Long> q = expandNode(parentID, direction);
            List<Long> q = expandNodeAsList(parentID,direction);

            for(Long l:q) {
                returnSet.add(l);
            }
            // returnSet.addAll(q); besser?

            currentPos++;
        }

    }
}


