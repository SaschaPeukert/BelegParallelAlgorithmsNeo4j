package de.saschapeukert.Algorithms.Impl.RandomWalk;

import de.saschapeukert.Algorithms.Abst.MyAlgorithmBaseCallable;
import de.saschapeukert.Starter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the Random Walk Algorithm using the Core API of Neo4j
 * <br>
 * Created by Sascha Peukert on 03.08.2015.
 */
public class RandomWalkCoreApiAlgorithmCallable extends MyAlgorithmBaseCallable {

    private final int _RandomNodeParameter;
    private  Node currentNode;
    private int NUMBER_OF_STEPS;
    private final ThreadLocalRandom random;

    public RandomWalkCoreApiAlgorithmCallable(int NumberOfSteps, TimeUnit tu){
        super(tu);

        this._RandomNodeParameter = Starter.RANDOMWALKRANDOM;
        this.currentNode = null;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = ThreadLocalRandom.current();
    }

    @Override
    public void work() {

        timer.start();
        this.tx = db.openTransaction();

        while (this.NUMBER_OF_STEPS > 0) {

            int w = random.nextInt(100) + 1;
            if (w <= _RandomNodeParameter) {
                currentNode = db.getSomeRandomNode(random);
            } else {
                currentNode = getNextNode(currentNode);
            }
            Starter.incrementResultCounterforId(currentNode.getId());
            NUMBER_OF_STEPS--;
        }
        db.closeTransactionWithSuccess(tx);
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
            }
        }
        return db.getSomeRandomNode( random);  // Node has no outgoing relationships or is start "node"
    }
}


