package de.saschapeukert;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 17.09.2015.
 */
public class DFS {

    private Node currentNode;
    private int id;
    private List<Long> visited;

    public DFS(Node n, int id){
        this.currentNode = n;
        this.id = id;
        this.visited = new LinkedList<Long>();
    }

    public void go(int barrier){

        if(barrier<=0)
            return;

        int oldID= StartComparison.resultCounter.get(currentNode.getId()).intValue();
        if(oldID!=0){
            // Already visted

            if(oldID!=id){
                // not by myself! Rewrite!
                id = oldID;
                for(Long k:visited){
                    StartComparison.resultCounter.put(k,new AtomicInteger(id));
                }

            } // else: by myself

            return;
        }
        // else: not yet visited. Lets do that now:

        visited.add(currentNode.getId());
        StartComparison.resultCounter.put(currentNode.getId(), new AtomicInteger(id));
        ConnectedComponentsSingleThreadAlgorithm.allNodes.remove(currentNode); // correct?   notwendig?!

        Node oldCurrent = currentNode;
        int barrier_new = barrier-1;
        for(Relationship r :currentNode.getRelationships()){
            currentNode = r.getOtherNode(oldCurrent);
            go(barrier_new);

        }

    }
}
