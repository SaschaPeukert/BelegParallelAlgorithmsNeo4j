package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.MyBFS;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */


@SuppressWarnings("deprecation")
public class ConnectedComponentsMultiThreadAlgorithm extends AbstractConnectedComponents {

    public ConnectedComponentsMultiThreadAlgorithm(CCAlgorithmType type, boolean output){
        super(type, output);

    }


    @Override
    public void compute() {

        super.compute();

        if(myType==CCAlgorithmType.WEAK){
            MyBFS.createInstance().closeDownThreads();
        }
    }


    @Override
    protected void weakly(Long n, int compName){

        MyBFS bfs = MyBFS.createInstance();
        Set<Long> reachableIDs = bfs.work(n, Direction.BOTH);


        for(Long l:reachableIDs){
            StartComparison.putIntoResultCounter(l, new AtomicInteger(compName));
            allNodes.remove(l);
        }

    }
    @Override
    protected void strongly(Long n){
        // NOT YET IMPLEMENTED
        throw new NotImplementedException();

    }


}

