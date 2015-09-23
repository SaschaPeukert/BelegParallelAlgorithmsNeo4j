package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.*;
import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Datastructures.TarjanNode;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */


@SuppressWarnings("deprecation")
public class ConnectedComponentsSingleThreadAlgorithm extends MyAlgorithmBaseRunnable {

    public enum AlgorithmType{
        WEAK,
        STRONG
    }


    private int componentID;
    private final AlgorithmType myType;

    private Map<Long,TarjanNode> nodeDictionary;
    private Stack<Long> stack;
    private int maxdfs=0;

    public static ReadOperations ops; //TODO REFACTOR THIS

    public static Set<Long> allNodes;


    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb, int highestNodeId
            , AlgorithmType type, boolean output){
        super(gdb, highestNodeId, output);

        this.myType = type;

        allNodes = new HashSet<>(highestNodeId);

        if(myType==AlgorithmType.STRONG) {
            // initialize nodeDictionary for tarjans algo
            this.stack = new Stack<>();
            this.nodeDictionary = new HashMap<>(highestNodeId);
        }
        tx = DBUtils.openTransaction(graphDb);
        GlobalGraphOperations ggop = GlobalGraphOperations.at(gdb);
        ggop.getAllNodes().iterator();

        ResourceIterator<Node> it = ggop.getAllNodes().iterator();
        while(it.hasNext()){
            Node n = it.next();
            allNodes.add(n.getId());

            if(myType==AlgorithmType.STRONG)
                nodeDictionary.put(n.getId(),new TarjanNode());

        }

        it.close();
        DBUtils.closeTransactionSuccess(tx);
    }


    @Override
    public void compute() {

        timer.start();
        componentID = 1;

        tx = DBUtils.openTransaction(graphDb);

        ThreadToStatementContextBridge ctx = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);

        ops = ctx.get().readOperations();

        Iterator<Long> it = allNodes.iterator();
        //DFS dfs = new DFS(this.highestNodeId);
        while(it.hasNext()){
            // Every node has to be marked as (part of) a component
            it = allNodes.iterator();

            try {
                Long n = it.next();
                if(myType==AlgorithmType.WEAK){

                    //dfs.setCurrentNodeID(n);
                    //dfs.setId(componentID);
                    //dfs.resetList();
                    //dfs.go(100);  // just to try it
                    DFS(n,componentID);
                    componentID++;
                    //System.out.println(allNodes.size());

                } else{
                    //System.out.println(allNodes.size());  // just for me TODO Remove!
                    tarjan(n);
                }

            }catch (NoSuchElementException e){
                break;
            }

        }
        DBUtils.closeTransactionSuccess(tx);

        timer.stop();
    }

    private void tarjan(Long currentNode){

        TarjanNode v = nodeDictionary.get(currentNode);
        v.dfs = maxdfs;
        v.lowlink = maxdfs;
        maxdfs++;

        v.onStack = true;           // This should be atomic
        stack.push(currentNode);        // !

        //itN.remove();
        allNodes.remove(currentNode);

        //Iterable<Relationship> it = currentNode.getRelationships(Direction.OUTGOING);
        //for(Relationship r: it){
        //   Node n_new = r.getOtherNode(currentNode);

        Iterable<Long> it = DBUtils.getConnectedNodeIDs(ops, currentNode, Direction.OUTGOING);
        for(Long l:it){

            TarjanNode v_new = nodeDictionary.get(l);

            if(allNodes.contains(l)){
                tarjan(l);

                v.lowlink = Math.min(v.lowlink,v_new.lowlink);

            } else if(v_new.onStack){       // O(1)

                v.lowlink = Math.min(v.lowlink,v_new.dfs);
            }

        }

        if(v.lowlink == v.dfs){
            // Root of a SCC

            while(true){
                Long node_v = stack.pop();                      // This should be atomic
                TarjanNode v_new = nodeDictionary.get(node_v);  // !
                v_new.onStack= false;                           // !

                StartComparison.putIntoResultCounter(node_v, new AtomicInteger(componentID));
                if(Objects.equals(node_v, currentNode)){
                    componentID++;
                    break;
                }

            }
        }

    }

    private void DFS(Long n, int compName){

        if(StartComparison.getResultCounterforId(n).intValue()==compName){
            return;// Already visited
        }

        // NOW IT HAS TO BE NULL
        StartComparison.putIntoResultCounter(n, new AtomicInteger(compName));
        allNodes.remove(n); // correct?   notwendig?!

        for(Long l: DBUtils.getConnectedNodeIDs(ConnectedComponentsSingleThreadAlgorithm.ops, n, Direction.BOTH)){
            DFS(l, compName);

        }

    }


    public String getResults(){

        Map<Integer, List<Long>> myResults = new TreeMap<>();

        // to adapt to the "old" structure of componentsMap
        Iterator<Long> it = StartComparison.getIteratorforKeySetOfResultCounter();
        while(it.hasNext()){
            long n = it.next();
            if(!myResults.containsKey(StartComparison.getResultCounterforId(n).intValue())){

                ArrayList<Long> newList = new ArrayList<>();
                newList.add(n);
                myResults.put(StartComparison.getResultCounterforId(n).intValue(),newList);
            } else{
                List<Long> oldList = myResults.get(StartComparison.getResultCounterforId(n).intValue());
                oldList.add(n);
                myResults.put(StartComparison.getResultCounterforId(n).intValue(),oldList);
            }
        }

        // Building the result string

        StringBuilder returnString = new StringBuilder();
        returnString.append("Component count: ").append(myResults.keySet().size()).append("\n");
        returnString.append("Components with Size between 4 and 10\n");
        returnString.append("- - - - - - - -\n");
        for(Integer s:myResults.keySet()){
            if((myResults.get(s).size()<=5) || (myResults.get(s).size()>=10)){
                continue;
            }

            boolean first = true;
            returnString.append("Component ").append(s).append(": ");
            for(Long n:myResults.get(s)){
                if(!first){
                    returnString.append(", ");
                } else{
                    first = false;
                }
                returnString.append(n);
            }
            returnString.append("\n");
        }

        returnString.append("- - - - - - - -\n");
        returnString.append("Done in: ").append(timer.elapsed(TimeUnit.MICROSECONDS)).append("\u00B5s");

        return returnString.toString();
    }

}

