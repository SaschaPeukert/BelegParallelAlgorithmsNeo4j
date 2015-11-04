package de.saschapeukert;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Sascha Peukert on 21.09.2015.
 *
 * This class is used for debugging and displaying results
 */
public class OutputTop20 {

    public static void main(String[] args) {

        String DB_PATH;
        String PAGECACHE;
        String PROP_NAME;
        Transaction tx;

        // READING THE INPUTPARAMETER
        try {
            PROP_NAME = args[0];
            PAGECACHE = args[1];
            DB_PATH = args[2];

        } catch (Exception e) {
            System.out.println("Not enough input parameter.");
            System.out.println("You have to supply: " +
                    " PropertyName PageCache(String)_in_G/M/K DB-Path");
            return;
        }

        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(DB_PATH))
                .setConfig(GraphDatabaseSettings.pagecache_memory, PAGECACHE)
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "false")  // to get rid of all those neostore.trasaction.db ... files
                .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
                .newGraphDatabase();

        tx = DBUtils.getInstance("", "").openTransaction();  // Does this work anymore??!?!?! TESTME -> FIXME
        DBUtils.getInstance("", "").closeTransactionWithSuccess(tx);
        graphDb.shutdown();

        System.out.println(printOutput(getTop20(PROP_NAME)));
    }

    public static List<Object[]> getTop20(String propName){
        String query = "MATCH (n) WHERE n." + propName
                + ">0 RETURN id(n),n." + propName + " ORDER BY n." + propName + " DESC LIMIT 20";
        Result result = DBUtils.getInstance("","").executeQuery(query);
        List<Object[]> resultList = new ArrayList<>();

        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            Object[] id_value = new Object[2];
            int i=0;

            for (Map.Entry<String, Object> column : row.entrySet()) {
                id_value[i]=column.getValue();
                i++;
            }
            resultList.add(id_value);
        }
        return resultList;
    }

    private static String printOutput(List<Object[]> list){
        String result="";
        for(Object[] objs:list){
            result +=  "id("+objs[0] +") => "+ objs[1] + "\n";
        }
        return result;
    }
}
