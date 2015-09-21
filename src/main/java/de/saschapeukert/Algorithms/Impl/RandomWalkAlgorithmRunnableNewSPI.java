package de.saschapeukert.Algorithms.Impl;

import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.concurrent.ThreadLocalRandom;

//import org.neo4j.kernel.api.Cursor.RelationshipItem;

/**
 * Created by Sascha Peukert on 03.08.2015.
 */


public class RandomWalkAlgorithmRunnableNewSPI extends MyAlgorithmBaseRunnable {


    private final int _RandomNodeParameter;
    private long currentNodeId;
    private int NUMBER_OF_STEPS;
    private final ThreadLocalRandom random;
    private final ThreadToStatementContextBridge ctx;
    private ReadOperations ops;

    public RandomWalkAlgorithmRunnableNewSPI(int randomChanceParameter,
                                             GraphDatabaseService gdb,int highestNodeId,int NumberOfSteps, boolean output){
        super(gdb, highestNodeId, output);

        this._RandomNodeParameter = randomChanceParameter;
        this.currentNodeId = -1;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = ThreadLocalRandom.current();
        GraphDatabaseAPI api = (GraphDatabaseAPI) gdb;

        this.ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);


        /*
        PrimitiveLongIterator p =  ctx.get().readOperations().nodesGetAll();
        Cursor<NodeItem> allNodesCursor = ctx.get().readOperations().nodeCursorGetAll();

        while (allNodesCursor.next()) {
            NodeItem nodeItem = allNodesCursor.get();
            nodeItem.getRelationships(Direction.BOTH);
        }
        int i=0;
        while(p.hasNext()){
            i++;
            p.next();
        }
        */
        /*try {
            Iterator<DefinedProperty> intIt = ctx.get().readOperations().node

            while(intIt.hasNext()){
                System.out.println((intIt.next().valueAsString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }*/




    }
    @Override
    public void compute() {

        timer.start();

            this.ops = ctx.get().readOperations();

            while (this.NUMBER_OF_STEPS > 0) {

                int w = random.nextInt(100) + 1;
                if (w <= _RandomNodeParameter) {
                    currentNodeId = DBUtils.getSomeRandomNodeId(random, highestNodeId);
                } else{
                    currentNodeId = getNextNode(currentNodeId);
                }

                if(output)
                    StartComparison.resultCounter.get(currentNodeId).incrementAndGet();


                NUMBER_OF_STEPS--;
            }



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
        return DBUtils.getSomeRandomNodeId(random, highestNodeId);  // Node has no outgoing relationships or is start "node"
    }


}


