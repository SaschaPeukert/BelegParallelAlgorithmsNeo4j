package de.saschapeukert.Database;

import de.saschapeukert.StartComparison;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Sascha Peukert on 31.08.2015.
 */
@SuppressWarnings("deprecation")
public class DBUtils {


    private static StoreAccess neoStore;

    public static Node getSomeRandomNode(GraphDatabaseService graphDb, ThreadLocalRandom random, int highestNodeId){
        long r;
        while(true) {

            try {

                // NEW VERSION, checks Map for ID and not DB
                r = (long) random.nextInt(highestNodeId);
                if(StartComparison.resultCounter.containsKey(r)){
                    return graphDb.getNodeById(r);
                }

            } catch (NotFoundException e){
                // NEXT! OLD VERSION
                // NEW: this should never be happening!
                System.out.println("Something terrible is happend");
            }

        }

    }

    public static long getSomeRandomNodeId(ThreadLocalRandom random, int highestNodeId){
        long r;
        while(true) {

            r = (long) random.nextInt(highestNodeId);

            // NEW VERSION without DB-Lookup
            if(StartComparison.resultCounter.containsKey(r))
                return r;

            // OLD VERSION
            //if(ops.nodeExists(r))
            //    return r;

        }

    }

    public static Relationship getSomeRandomRelationship(GraphDatabaseService graphDb, ThreadLocalRandom random, int highestNodeId){
        long r;
        while(true) {

            try {
                r = (long) random.nextInt(highestNodeId);
                Node n = graphDb.getNodeById(r);  // meh?
                return n.getRelationships(Direction.BOTH).iterator().next();
            } catch (NotFoundException | NoSuchElementException ex){
                // NEXT!
            }

        }

    }

    public static boolean removePropertyFromAllNodes(int PropertyID, DataWriteOperations ops, GraphDatabaseService gdb){
        //int i=0;
        Iterator<Node> it = getIteratorForAllNodes(gdb);
        try {
            while(it.hasNext()){
                ops.nodeRemoveProperty(it.next().getId(),PropertyID);
                //i++;
                //if(i%250000==0)
                //    System.out.println(i);
            }


        } catch (EntityNotFoundException e) {
            e.printStackTrace(); // TODO REMOVE
            return false;
        }

        return true;
    }

    /**
     *  ONLY REMOVES THE KEY!
     *  You have to remove the Property from every node too
     * @param propertyID
     * @param ops
     * @return
     */
    public static void removePropertyKey(int propertyID, DataWriteOperations ops){
        ops.graphRemoveProperty(propertyID);

    }

    public static boolean createStringPropertyAtNode(long nodeID, String value, int PropertyID, DataWriteOperations ops){

        try {
            ops.nodeSetProperty(nodeID, Property.stringProperty(PropertyID, value));
        } catch (EntityNotFoundException | ConstraintValidationKernelException e) {
            e.printStackTrace(); // TODO REMOVE
            return false;
        }

        return true;
    }

    public static boolean createIntPropertyAtNode(long nodeID, int value, int PropertyID, DataWriteOperations ops){

        try {
            ops.nodeSetProperty(nodeID, Property.intProperty(PropertyID, value));
        } catch (EntityNotFoundException | ConstraintValidationKernelException e) {
            e.printStackTrace(); // TODO REMOVE
            return false;
        }

        return true;
    }

    /**
     *
     * @param key HAS TO BE UNIQUE!
     * @param newPropertyID
     * @param ops
     * @return
     */
    public static boolean createNewPropertyKey(String key,int newPropertyID, DataWriteOperations ops){
        try{
            ops.propertyKeyCreateForName(key, newPropertyID);
        } catch (IllegalTokenNameException e) {
            e.printStackTrace();  // TODO REMOVE
            return false;
        }
        return true;

    }

    public static int getHighestNodeID(GraphDatabaseService graphDb ){

        return (int) getStoreAcess(graphDb).getNodeStore().getHighId();

    }

    public static int getHighestPropertyID(GraphDatabaseService graphDb){

        return (int) getStoreAcess(graphDb).getPropertyStore().getHighId();

    }

    private static StoreAccess getStoreAcess(GraphDatabaseService graphDb){
        if(neoStore==null)
            neoStore = new StoreAccess((GraphDatabaseAPI)graphDb).initialize();
        return neoStore;
    }

    public static ResourceIterator<Node> getIteratorForAllNodes( GraphDatabaseService gdb) {
        GlobalGraphOperations ggo = GlobalGraphOperations.at(gdb);

        ResourceIterable<Node> allNodes = ggo.getAllNodes();
        return allNodes.iterator();

    }

    /**
     * Gets the PropertyID for a given PropertyName or creates a new ID for that name and returns it.
     * @param propertyName
     * @param highestPropertyKey
     * @param graphDb
     * @return -1 if error happend
     */
    public static int GetPropertyID(String propertyName, int highestPropertyKey, GraphDatabaseService graphDb){
        try(Transaction tx = graphDb.beginTx()) {

            ThreadToStatementContextBridge ctx = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            DataWriteOperations ops = ctx.get().dataWriteOperations();

            int lookup = ops.propertyKeyGetForName(propertyName);
            if(lookup==-1){
                System.out.println("Property not found. Creating new ID");
                highestPropertyKey++;
                createNewPropertyKey(propertyName, highestPropertyKey, ops);

                tx.success();

                return highestPropertyKey;  // can be used now, referencing now the correct PropertyKey.
                                            // No Node has a Property with this key jet

            } else{
                System.out.println("Property found. Using old ID " + lookup);
                //DBUtils.removePropertyFromAllNodes(lookup, ops, graphDb);  useless since every one of this properties will be overwritten later
                //System.out.println("Finished clearing up.");
                tx.success();

                return lookup;
            }

        } catch (InvalidTransactionTypeKernelException e) {
            e.printStackTrace();  // TODO REMOVE
        }

        return -1; // ERROR happend

    }


    public static Iterable<Long> getConnectedNodeIDs(ReadOperations ops, long nodeID, Direction dir){

        LinkedList<Long> it = new LinkedList<>();
        try {
            RelationshipIterator itR = ops.nodeGetRelationships(nodeID, dir);
            while(itR.hasNext()){
                long rID = itR.next();

                Cursor<RelationshipItem> relCursor = ops.relationshipCursor(rID);
                while(relCursor.next()){
                    RelationshipItem item = relCursor.get();
                    it.add(item.otherNode(nodeID));
                }
                relCursor.close();

            }


        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }

        return it;

    }

    public static Transaction openTransaction(GraphDatabaseService graphDb){
        return graphDb.beginTx();

    }

    public static void closeTransactionSuccess(Transaction tx){
        tx.success();
        tx.close();
    }
}
