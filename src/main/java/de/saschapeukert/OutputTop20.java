package de.saschapeukert;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sascha Peukert on 21.09.2015.
 *
 * This class is used for debugging and displaying results
 */
class OutputTop20 {

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
                        //  .setConfig(GraphDatabaseSettings.read_only,"true")
                .newGraphDatabase();


        tx = DBUtils.openTransaction(graphDb);



        System.out.println(
                printOutput(
                        getTop20(PROP_NAME,graphDb)));

        DBUtils.closeTransactionSuccess(tx);
    }

    public static Map<String, Integer> getTop20(String propName, GraphDatabaseService graphDb){
        Result result = graphDb.execute("MATCH (n) WHERE n." + propName
                + ">0 RETURN id(n),n." + propName + " ORDER BY n." + propName + " DESC LIMIT 20");
        Map<String,Integer> resultMap = new HashMap<>(20);
        //String rows = "";
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            for (Map.Entry<String, Object> column : row.entrySet()) {
                //rows += column.getKey() + ": " + column.getValue() + "; ";
                resultMap.put(column.getKey(),(int)column.getValue());
            }
            //rows += "\n";
        }

        return resultMap;
    }

    private static String printOutput(Map<String,Integer> map){
        String result="";

        for(String s:map.keySet()){
            result += s +" => " + map.get(s) + "\n";
        }

        return result;

    }
}
