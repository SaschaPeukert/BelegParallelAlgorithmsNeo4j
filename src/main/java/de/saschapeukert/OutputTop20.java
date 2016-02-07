package de.saschapeukert;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used for debugging and displaying results
 * <br>
 * Created by Sascha Peukert on 21.09.2015.
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
        DBUtils db = DBUtils.getInstance(DB_PATH, PAGECACHE);
        tx = db.openTransaction();
        db.closeTransactionWithSuccess(tx);

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
