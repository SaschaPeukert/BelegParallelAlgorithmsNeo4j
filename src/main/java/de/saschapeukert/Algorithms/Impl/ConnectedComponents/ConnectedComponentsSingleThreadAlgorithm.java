package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Datastructures.TarjanNode;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */


@SuppressWarnings("deprecation")
public class ConnectedComponentsSingleThreadAlgorithm extends AbstractConnectedComponents {

    public ConnectedComponentsSingleThreadAlgorithm(CCAlgorithmType type, boolean output){
        super(type, output);

    }

    @Override
    protected void strongly(Long currentNode){

        TarjanNode v = nodeDictionary.get(currentNode);
        v.dfs = maxdfs;
        v.lowlink = maxdfs;
        maxdfs++;

        v.onStack = true;           // This should be atomic
        stack.push(currentNode);        // !

        allNodes.remove(currentNode);

        Iterable<Long> it = db.getConnectedNodeIDs(db.getReadOperations(), currentNode, Direction.OUTGOING);
        for(Long l:it){

            TarjanNode v_new = nodeDictionary.get(l);

            if(allNodes.contains(l)){
                strongly(l);

                v.lowlink = Math.min(v.lowlink,v_new.lowlink);

            } else if(v_new.onStack){       // O(1)

                v.lowlink = Math.min(v.lowlink,v_new.dfs);
            }

        }

        if(v.lowlink == v.dfs){
            // Root of a SCC

            while(true){
                Long node_v = stack.pop();                      // This should be atomic
                TarjanNode v_new = nodeDictionary.get(node_v);  // !
                v_new.onStack= false;                           // !

                StartComparison.putIntoResultCounter(node_v, new AtomicInteger(componentID));
                if(Objects.equals(node_v, currentNode)){
                    componentID++;
                    break;
                }

            }
        }

    }

    protected void weakly(Long n, int compName){
        Set<Long> reachableIDs = BFS.go(n, Direction.BOTH);

        for(Long l:reachableIDs){
            StartComparison.putIntoResultCounter(l, new AtomicInteger(compName));
            allNodes.remove(l);
        }

    }


}

