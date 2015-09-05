package de.saschapeukert;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.Map;

/**
 * Created by Sascha Peukert on 05.09.2015.
 */
public class NeoWriter implements Runnable {

    private Map<Long, Integer> myMap;
    private GraphDatabaseService graphDb;
    private int propID;
    private Transaction tx;

    public NeoWriter(Map<Long,Integer> results, int propID, GraphDatabaseService graphDb){
        this.myMap = results;
        this.graphDb = graphDb;
        this.propID = propID;
    }

    @Override
    public void run() {

        try {
            tx = graphDb.beginTx();

            ThreadToStatementContextBridge ctx =((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            DataWriteOperations ops = ctx.get().dataWriteOperations();

            for (Long l : myMap.keySet()) {
                DBUtils.createIntPropertyAtNode(l, myMap.get(l), propID, ops);
            }

            //tx.success();

        } catch (InvalidTransactionTypeKernelException e) {
            e.printStackTrace(); // TODO remove

        }


    }

    public void commitTransaction(){
        tx.success();
        tx.close();
    }
}
