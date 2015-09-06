package de.saschapeukert;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Sascha Peukert on 31.08.2015.
 */
public class DBUtils {

    public static Node getSomeRandomNode(GraphDatabaseService graphDb, ThreadLocalRandom random, int highestNodeId){
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

    public static long getSomeRandomNodeId(ReadOperations ops, ThreadLocalRandom random, int highestNodeId){
        long r;
        while(true) {

            r = (long) random.nextInt(highestNodeId);
            if(ops.nodeExists(r))
                return r;

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
    public static boolean removePropertyKey(int propertyID, DataWriteOperations ops){
        ops.graphRemoveProperty(propertyID);
        return true;
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
            ops.propertyKeyCreateForName(key,newPropertyID);
        } catch (IllegalTokenNameException e) {
            e.printStackTrace();  // TODO REMOVE
            return false;
        }
        return true;

    }

    public static int getHighestNodeID(GraphDatabaseService graphDb ){

        // TODO refactoring?
        NeoStore neoStore = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(NeoStore.class);
        return (int) neoStore.getNodeStore().getHighId();

    }

    public static int getHighestPropertyID(GraphDatabaseService graphDb ){

        // TODO refactoring?
        NeoStore neoStore = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(NeoStore.class);
        return (int) neoStore.getPropertyStore().getHighId();

    }

    public static Iterator<Node> getIteratorForAllNodes( GraphDatabaseService gdb) {
        GlobalGraphOperations ggo = GlobalGraphOperations.at(gdb);

        ResourceIterable<Node> allNodes = ggo.getAllNodes();
        Iterator<Node> it = allNodes.iterator();
        return it;

    }
}
