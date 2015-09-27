package de.saschapeukert.Database;

import de.saschapeukert.StartComparison;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.File;
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
    private static GraphDatabaseService graphDb;
    public static int highestPropertyKey;
    public static int highestNodeKey;

    public static Node getSomeRandomNode(GraphDatabaseService graphDb, ThreadLocalRandom random){
        long r;
        while(true) {

            try {

                // NEW VERSION, checks Map for ID and not DB
                r = (long) random.nextInt(highestNodeKey);
                if(StartComparison.resultCounterContainsKey(r)){
                    return graphDb.getNodeById(r);
                }

            } catch (NotFoundException e){
                // NEXT! OLD VERSION
                // NEW: this should never be happening!
                System.out.println("Something terrible is happend");
            }

        }

    }

    public static long getSomeRandomNodeId(ThreadLocalRandom random){
        long r;
        while(true) {

            r = (long) random.nextInt(highestNodeKey);

            // NEW VERSION without DB-Lookup
            if(StartComparison.resultCounterContainsKey(r))
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

    public static int getHighestNodeID(GraphDatabaseService graphDb ){

        return (int) getStoreAcess(graphDb).getNodeStore().getHighId();

    }

    public static int getHighestPropertyID(GraphDatabaseService graphDb){
        return (int) getStoreAcess(graphDb).getPropertyStore().getHighId();

    }

    private static long getNextPropertyID(GraphDatabaseService graphDb){
        return getStoreAcess(graphDb).getPropertyStore().nextId();
    }

    private static StoreAccess getStoreAcess(GraphDatabaseService graphDb){
        if(neoStore==null)
            neoStore = new StoreAccess(((GraphDatabaseAPI)graphDb).getDependencyResolver().resolveDependency( NeoStore.class ));
        return neoStore;
    }

    public static ResourceIterator<Node> getIteratorForAllNodes( GraphDatabaseService gdb) {
        GlobalGraphOperations ggo = GlobalGraphOperations.at(gdb);

        ResourceIterable<Node> allNodes = ggo.getAllNodes();
        return allNodes.iterator();

    }

    /**
     * Gets the PropertyID for a given PropertyName or creates a new ID for that name and returns it.
     * @param propertyName HAS TO BE UNIQUE
     * @return -1 if error happend
     */
    public static int GetPropertyID(String propertyName){
        try(Transaction tx = graphDb.beginTx()) {

            ThreadToStatementContextBridge ctx = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            DataWriteOperations ops = ctx.get().dataWriteOperations();

            int test = getHighestPropertyID(graphDb);
            return  ops.propertyKeyGetOrCreateForName(propertyName);

        } catch (Exception e) {
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

    public static void closeTransactionWithSuccess(Transaction tx){
        tx.success();
        tx.close();
    }

    public static GraphDatabaseService getGraphDb(String path, String pagecache) {
        if(graphDb==null){
            graphDb = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder(new File(path))
                    .setConfig(GraphDatabaseSettings.pagecache_memory, pagecache)
                    .setConfig(GraphDatabaseSettings.keep_logical_logs, "false")  // to get rid of all those neostore.trasaction.db ... files
                    .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
                    .newGraphDatabase();

            registerShutdownHook(graphDb);

            highestNodeKey = getHighestNodeID(graphDb);
            highestPropertyKey = getHighestPropertyID(graphDb);
        }


        return graphDb;

    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down neo4j.");
                try {
                    graphDb.shutdown();
                } catch (Exception e) {
                    graphDb.shutdown();
                } finally {
                }
                System.out.println("Shutting down neo4j complete.");

            }
        });
    }

}
