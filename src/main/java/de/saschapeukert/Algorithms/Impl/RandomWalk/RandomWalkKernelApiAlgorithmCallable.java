package de.saschapeukert.Algorithms.Impl.RandomWalk;

import de.saschapeukert.Algorithms.Abst.MyAlgorithmBaseCallable;
import de.saschapeukert.Starter;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the Random Walk Algorithm using the Kernel API of Neo4j
 * <br>
 * Created by Sascha Peukert on 03.08.2015.
 */

public class RandomWalkKernelApiAlgorithmCallable extends MyAlgorithmBaseCallable {

    private final int _RandomNodeParameter;
    private long currentNodeId;
    private int NUMBER_OF_STEPS;
    private final ThreadLocalRandom random;
    private ReadOperations ops;

    public RandomWalkKernelApiAlgorithmCallable(int NumberOfSteps, TimeUnit tu){
        super(tu);

        this._RandomNodeParameter = Starter.RANDOMWALKRANDOM;
        this.currentNodeId = -1;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = ThreadLocalRandom.current();
    }
    @Override
    public void work() {

        timer.start();
        this.tx = db.openTransaction();
        this.ops = db.getReadOperations();

        while (this.NUMBER_OF_STEPS > 0) {
            int w = random.nextInt(100) + 1;
            if (w <= _RandomNodeParameter) {
                currentNodeId = db.getSomeRandomNodeId(random);
            } else{
                currentNodeId = getNextNode(currentNodeId);
            }
            Starter.incrementResultCounterforId(currentNodeId);
            NUMBER_OF_STEPS--;
        }
        db.closeTransactionWithSuccess(tx);
        timer.stop();

    }

    private long getNextNode(long n){
        if (n != -1) {
            int relationshipsOfNode;

            try {
                relationshipsOfNode = ops.nodeGetDegree(n, Direction.OUTGOING);
                if (relationshipsOfNode > 0) {
                    // Choose one of the relationships to follow
                    RelationshipIterator itR = ops.nodeGetRelationships(n, Direction.OUTGOING);
                    int new_relationshipIndex = random.nextInt(relationshipsOfNode);

                    for (int i = 0; i <= new_relationshipIndex; i++) {
                        if (i == new_relationshipIndex) {
                            long r = itR.next();

                            Cursor<RelationshipItem> relCursor = ops.relationshipCursor(r);//id);
                            RelationshipItem item = relCursor.get();
                            if (relCursor.next()) {
                                return item.otherNode(n);
                            }
                        } else {
                            itR.next();
                        }
                    }
                }
            } catch (EntityNotFoundException e) {
                e.printStackTrace();
                return -1; // ERROR!
            }
        }
        return db.getSomeRandomNodeId(random);  // Node has no outgoing relationships or is start "node"
    }
}


