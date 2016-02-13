package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import de.saschapeukert.Algorithms.Abst.WorkerCallableTemplate;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * This class is needed by MyBFS to explore a given part of the frontier in parallel.
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFSLevelCallable extends WorkerCallableTemplate {

    public Direction direction;

    public MyBFSLevelCallable(int startPos, int endPos, long[] array, Direction direction){
        super(startPos,endPos,array);
        this.direction = direction;
    }

    @Override
    public void work() {

        int currentPos=startPos;
        while(currentPos<endPos){
            long parentID = refArray[currentPos];
            //Set<Long> q = expandNode(parentID, direction);
            LongArrayList q = expandNodeAsList(parentID,direction);

            Iterator<LongCursor> it = q.iterator();
            while(it.hasNext()){
                Long l = it.next().value;
                returnList.add(l);
            }
            // returnList.addAll(q); besser?

            currentPos++;
        }

    }
}


