package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Algorithms.Abst.WorkerRecursiveTaskTemplate;
import de.saschapeukert.Starter;
import org.neo4j.graphdb.Direction;

import java.util.List;

/**
 * This class is needed by MyBFS to explore a given part of the frontier in parallel.
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public class MyBFSLevelRecursiveTask extends WorkerRecursiveTaskTemplate {

    public Direction direction;

    public MyBFSLevelRecursiveTask(int startPos, int endPos, Long[] array, Direction direction){
        super(startPos,endPos,array);
        this.direction = direction;
    }

    @Override
    public void work() {

        int currentPos=startPos;
        MyBFSLevelRecursiveTask left=null;
        if(endPos-currentPos >= Starter.BATCHSIZE*2) {
            // Fork
            int middle = (endPos - currentPos) / 2;
            left = new MyBFSLevelRecursiveTask(startPos, middle, refArray, direction);
            left.fork();
            currentPos = middle;
        }
        while(currentPos<endPos){
                // Work
                currentPos = expand(currentPos);
        }

        if(left!=null){
            returnSet.addAll((List<Long>)left.join());
        }
    }



    private int expand(int currentPos){
        long parentID = refArray[currentPos];
        List<Long> q = expandNodeAsList(parentID,direction);
        for(Long l:q) {
            returnSet.add(l);
        }
        currentPos++;
        return currentPos;
    }
}


