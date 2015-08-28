package de.saschapeukert;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Sascha Peukert on 03.08.2015.
 */
public class RandomWalkAlgorithmRunnable extends AlgorithmRunnable {

//    public String Protocol;
    public int _RandomNodeParameter;
    private Node currentNode;
    private int NUMBER_OF_STEPS;
    private ThreadLocalRandom random;

    public RandomWalkAlgorithmRunnable(int randomChanceParameter,
                                       GraphDatabaseService gdb,int highestNodeId, int NumberOfSteps){
        super(gdb, highestNodeId);

//        this.Protocol = "";
        this._RandomNodeParameter = randomChanceParameter;
        this.currentNode = null;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = ThreadLocalRandom.current();
    }
    @Override
    public void compute() {


/*
        TraversalDescription traversalDescription = graphDb.traversalDescription().depthFirst().expand(new PathExpander<AtomicInteger>() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState branchState) {
                Relationship rel = path.endNode().getRelationships();
                return Collections.singleton(rel);
            }

            @Override
            public PathExpander<AtomicInteger> reverse() {
                return null;
            }
        }).evaluator(new PathEvaluator() {
            @Override
            public Evaluation evaluate(Path path, BranchState<> branchState) {
                return path.length() < NUMBER_OF_STEPS ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_PRUNE;
            }

            @Override
            public Evaluation evaluate(Path path) {
                return null;
            }
        });

        org.neo4j.graphdb.traversal.Traverser traverse = traversalDescription.traverse(startNode);

        for (Path path : traverse) {

        }
*/

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
                        List<Relationship> relationships =
                                getListOfOutgoingRelationships(currentNode);
                        w = random.nextInt(relationships.size());
                        currentNode = relationships.get(w).getEndNode();

                    }

                }

                // Protocol the newly reached node
                graphDb.getNodeById(currentNode.getId()); // just a lookup to generate "work" for the transaction
//                this.Protocol += "\n" + timer.elapsed(TimeUnit.MICROSECONDS) + " \u00B5s: ID:" + currentNode.getId();

                NUMBER_OF_STEPS--;
            }

            tx.success();  // Important!
        }


        timer.stop();

    }

    private List<Relationship> getListOfOutgoingRelationships(Node n){
        List<Relationship> arr = new ArrayList<>(100);
        if (n != null) {
            Iterable<Relationship> it = n.getRelationships(Direction.OUTGOING);

            for(Relationship r:it) {
                arr.add(r);
            }
        }
        return arr;
    }

    private Node getSomeRandomNode(){
        long r;
        while(true) {

            try {
                r = (long) random.nextInt(highestNodeId);
                Node n = graphDb.getNodeById(r);
                return n;
            } catch (NotFoundException e){
                // NEXT!
            }


        }

    }

}


