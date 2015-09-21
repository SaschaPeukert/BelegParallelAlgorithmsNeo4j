package de.saschapeukert.Database;

import de.saschapeukert.Algorithms.MyBaseRunnable;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

/**
 * Created by Sascha Peukert on 05.09.2015.
 */
public class NeoWriter extends MyBaseRunnable {


    private final GraphDatabaseService graphDb;
    private final int propID;
    private final int startIndex; // inclusive
    private final int endIndex; // exclusive

    public NeoWriter( int propID, GraphDatabaseService graphDb, int startIndex, int endIndex){

        this.graphDb = graphDb;
        this.propID = propID;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    public void run() {

        try {
            tx = DBUtils.openTransaction(graphDb);

            ThreadToStatementContextBridge ctx =((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            DataWriteOperations ops = ctx.get().dataWriteOperations();

            int count =0;
            for(int i = startIndex;i<endIndex;i++){

                if(count==100){

                    DBUtils.closeTransactionSuccess(tx);

                    tx = DBUtils.openTransaction(graphDb);

                    //ctx =((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
                    ops = ctx.get().dataWriteOperations();

                    count =0;
                }

                Long l = (Long) StartComparison.keySetOfResultCounter[i];
                DBUtils.createIntPropertyAtNode(l, StartComparison.resultCounter.get(l).intValue(), propID, ops);
                count++;

            }

            DBUtils.closeTransactionSuccess(tx);
            //System.out.println("Done " + startIndex + " - " + endIndex);

        } catch (InvalidTransactionTypeKernelException e) {
            e.printStackTrace(); // TODO remove

        }

    }

}
