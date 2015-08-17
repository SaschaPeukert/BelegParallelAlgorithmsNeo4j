package de.saschapeukert;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 03.08.2015.
 */
public class RandomWalkAlgorithmRunnableSPI {//extends AlgorithmRunnable {
/*
    public String Protocol;
    public int _RandomNodeParameter;
    private long currentNode;
    private List<Long> allNodes;
    private int NUMBER_OF_STEPS;
    private Random random;
    private ThreadToStatementContextBridge ctx;


    public RandomWalkAlgorithmRunnableSPI(int randomChanceParameter, List<Long> allNodes,
                                          GraphDatabaseService gdb, int NumberOfSteps){
       super(gdb);

        this.Protocol = "";
        this._RandomNodeParameter = randomChanceParameter;
        this.currentNode = -100;
        this.allNodes = allNodes;
        this.NUMBER_OF_STEPS = NumberOfSteps;
        this.random = new Random();

        GraphDatabaseAPI api = ((GraphDatabaseAPI) graphDb);
       this.ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);


        PrimitiveLongIterator p =  ctx.get().readOperations().nodesGetAll();

        int i=0;
        while(p.hasNext()){
            i++;
            p.next();
        }

        try {
            Iterator<DefinedProperty> intIt = ctx.get().readOperations().nodeGetAllProperties(5);

            while(intIt.hasNext()){
                System.out.println((intIt.next().valueAsString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
     @Override
    public void compute() {

        timer.start();

        try (Transaction tx = graphDb.beginTx()) {
            while (this.NUMBER_OF_STEPS > 0) {
                if (currentNode == -100 || getListOfOutgoingRelationships(currentNode).size() == 0) {
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

    private List<Long> getListOfOutgoingRelationships(long n){
        ArrayList<Long> arr = new ArrayList<>();
        if (n != -100) {
            try {
                RelationshipIterator it = ctx.get().readOperations().nodeGetRelationships(n,Direction.OUTGOING);
                ctx.get().readOperations().re
                while(it.hasNext()) {
                    long relID = it.next();
                    Iterator<DefinedProperty> itDef = ctx.get().readOperations().relationshipGetAllProperties(relID);
                    while(itDef.hasNext()){
                        DefinedProperty d = itDef.next();


                    }
                    arr.add(1L);
                }
            } catch (EntityNotFoundException e) {
                e.printStackTrace();
            }

            //Iterable<Relationship> it = n.getRelationships(Direction.OUTGOING);

            for(Relationship r:it) {
                arr.add(r);
            }
        }
        return arr;
    }

    private long getSomeRandomNodeID(){
        int r = random.nextInt(allNodes.size());
        return allNodes.get(r);
    }
*/
}


