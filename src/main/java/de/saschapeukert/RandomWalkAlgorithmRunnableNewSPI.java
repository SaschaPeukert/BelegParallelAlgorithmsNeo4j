package de.saschapeukert;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

//import org.neo4j.kernel.api.Cursor.RelationshipItem;

/**
 * Created by Sascha Peukert on 03.08.2015.
 */


public class RandomWalkAlgorithmRunnableNewSPI extends AlgorithmRunnable {


    public String Protocol;
    public int _RandomNodeParameter;
    private Node currentNode;
    private int NUMBER_OF_STEPS;
    private Random random;
    private ThreadToStatementContextBridge ctx;

    public RandomWalkAlgorithmRunnableNewSPI(int randomChanceParameter, Set<Node> allNodes,
                                             GraphDatabaseService gdb, int NumberOfSteps){
        super(gdb,allNodes);

        this.Protocol = "";
        this._RandomNodeParameter = randomChanceParameter;
        this.currentNode = null;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = new Random();


        GraphDatabaseAPI api = ((GraphDatabaseAPI) graphDb);
        this.ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        //api.getDependencyResolver().resolveDependency(SchemaIndexProvider.class);



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

        /*try {
            Iterator<DefinedProperty> intIt = ctx.get().readOperations().node

            while(intIt.hasNext()){
                System.out.println((intIt.next().valueAsString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        try (Transaction tx = graphDb.beginTx()){
            getListOfReachableNodeIDs(50);
            tx.success();
        }



    }
    @Override
    public void compute() {

        timer.start();
        /*
        try (Transaction tx = graphDb.beginTx()) {
            while (this.NUMBER_OF_STEPS > 0) {
                if (currentNode == null || getListOfReachableNodeIDs(currentNode).size() == 0) {
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
                                (ArrayList<Relationship>) getListOfReachableNodeIDs(currentNode);
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
        */
        timer.stop();

    }

    private List<Long> getListOfReachableNodeIDs(long n){
        ArrayList<Long> arr = new ArrayList<>();
        if (n != -100) {
            try {
                ReadOperations ops = ctx.get().readOperations();

                RelationshipIterator it = ops.nodeGetRelationships(n, Direction.OUTGOING);

                while(it.hasNext()) {
                    long relID = it.next();


                    /*Cursor<RelationshipItem> relCursor = ops.relationshipCursor(relID);//id);
                    RelationshipItem item = relCursor.get();
                    if (relCursor.next()) {
                        arr.add(item.otherNode(n));
                    }*/
                    System.gc();
                }
            } catch (EntityNotFoundException e) {
                e.printStackTrace();
            }
        }
        return arr;

    }

    private Node getSomeRandomNode(){
        int r = random.nextInt(allNodes.size());
        return (Node) allNodes.toArray()[r];        // TODO: Better Way?
    }

}


