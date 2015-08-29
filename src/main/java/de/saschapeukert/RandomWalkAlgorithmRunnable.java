package de.saschapeukert;

import org.neo4j.graphdb.*;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Sascha Peukert on 03.08.2015.
 */
public class RandomWalkAlgorithmRunnable extends AlgorithmRunnable {

    public int _RandomNodeParameter;
    private Node currentNode;
    private int NUMBER_OF_STEPS;
    private ThreadLocalRandom random;

    public RandomWalkAlgorithmRunnable(int randomChanceParameter,
                                       GraphDatabaseService gdb,int highestNodeId, int NumberOfSteps){
        super(gdb, highestNodeId);

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

                int w = random.nextInt(100) + 1;
                if (w <= _RandomNodeParameter) {
                    currentNode = getSomeRandomNode();
                } else{
                    currentNode = getNextNode(currentNode);
                }

                NUMBER_OF_STEPS--;
            }

            tx.success();  // Important!
        }


        timer.stop();

    }

    private Node getNextNode(Node n){
        if (n != null) {
            int relationshipsOfNode = n.getDegree(Direction.OUTGOING);

            if(relationshipsOfNode>0){
                // Choose one of the relationships to follow
                Iterator<Relationship> itR = n.getRelationships(Direction.OUTGOING).iterator();

                int new_relationshipIndex = random.nextInt(relationshipsOfNode);

                for(int i=0;i<=new_relationshipIndex;i++)
                {
                    if(i==new_relationshipIndex){

                        Relationship r = itR.next();
                        return r.getOtherNode(n);

                    } else{
                        itR.next();
                    }
                }


            } else{
                // Node has no outgoing relationships
                return getSomeRandomNode();
            }

        }
        return null;  // Error!
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


