package de.saschapeukert;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
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


public class RandomWalkAlgorithmRunnableNewSPI extends AlgorithmRunnable {


    public String Protocol;
    public int _RandomNodeParameter;
    private long currentNodeId;
    private int NUMBER_OF_STEPS;
    private ThreadLocalRandom random;
    private ThreadToStatementContextBridge ctx;
    private ReadOperations ops;
    private GraphDatabaseAPI api;

    public RandomWalkAlgorithmRunnableNewSPI(int randomChanceParameter,
                                             GraphDatabaseService gdb,int highestNodeId,int pId,
                                             String pName,int NumberOfSteps){
        super(gdb, highestNodeId,pId, pName);

        this.Protocol = "";
        this._RandomNodeParameter = randomChanceParameter;
        this.currentNodeId = -1;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = ThreadLocalRandom.current();
        this.api = (GraphDatabaseAPI) gdb;

        //GraphDatabaseAPI api = ((GraphDatabaseAPI) graphDb);
        this.ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        //api.getDependencyResolver().resolveDependency(SchemaIndexProvider.class);


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

        try (Transaction tx = graphDb.beginTx()) {
            this.ops = ctx.get().readOperations();

            while (this.NUMBER_OF_STEPS > 0) {

                int w = random.nextInt(100) + 1;
                if (w <= _RandomNodeParameter) {
                    currentNodeId = DBUtils.getSomeRandomNodeId(ops, random, highestNodeId);
                } else{
                    currentNodeId = getNextNode(currentNodeId);
                }

                NUMBER_OF_STEPS--;
            }

            tx.success();  // Important!
           // tx.close();
        }

        timer.stop();

    }


    private long getNextNode(long n){
        if (n != -1) {
            int relationshipsOfNode = 0;

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
        return DBUtils.getSomeRandomNodeId(ops, random, highestNodeId);  // Node has no outgoing relationships or is start "node"
    }


}


