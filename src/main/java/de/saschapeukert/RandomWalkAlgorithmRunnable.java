package de.saschapeukert;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 03.08.2015.
 */
public class RandomWalkAlgorithmRunnable extends AlgorithmRunnable {

    public String Protocol;
    public int _RandomNodeParameter;
    private Node currentNode;
    private int NUMBER_OF_STEPS;
    private Random random;

    public RandomWalkAlgorithmRunnable(int randomChanceParameter, Set<Node> allNodes,
                                       GraphDatabaseService gdb, int NumberOfSteps){
        super(gdb,allNodes);

        this.Protocol = "";
        this._RandomNodeParameter = randomChanceParameter;
        this.currentNode = null;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = new Random();
    }
    @Override
    public void compute() {

        timer.start();

        try (Transaction tx = graphDb.beginTx()) {
            while (this.NUMBER_OF_STEPS > 0) {
                if (currentNode == null || getListOfOutgoingRelationships(currentNode).size() == 0) {
                    // "Start" or current node has no outgoing relationships
                    currentNode = getSomeRandomNode();
                } else {
                    int w = random.nextInt(100) + 1;
                    if (w <= _RandomNodeParameter) {
                        //this.Protocol += "\nI want to go somewhere completely else now!";
                        currentNode = getSomeRandomNode();
                    } else {
                        // Traverse one of the outgoing Relationships
                        ArrayList<Relationship> relationships =
                                (ArrayList<Relationship>) getListOfOutgoingRelationships(currentNode);
                        w = random.nextInt(relationships.size());
                        currentNode = relationships.get(w).getEndNode();

                    }

                }

                // Protocol the newly reached node
                graphDb.getNodeById(currentNode.getId()); // just a lookup to generate "work" for the transaction
                this.Protocol += "\n" + timer.elapsed(TimeUnit.MICROSECONDS) + " \u00B5s: ID:" + currentNode.getId();

                NUMBER_OF_STEPS--;
            }

            tx.success();  // Important!
        }

        timer.stop();

    }

    private List<Relationship> getListOfOutgoingRelationships(Node n){
        ArrayList<Relationship> arr = new ArrayList<>();
        if (n != null) {
            Iterable<Relationship> it = n.getRelationships(Direction.OUTGOING);

            for(Relationship r:it) {
                arr.add(r);
            }
        }
        return arr;
    }

    private Node getSomeRandomNode(){
        int r = random.nextInt(allNodes.size());
        return (Node) allNodes.toArray()[r];        // TODO: Better Way?
    }

}


