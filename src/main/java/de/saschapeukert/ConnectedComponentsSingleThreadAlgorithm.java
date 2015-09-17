package de.saschapeukert;

import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */


public class ConnectedComponentsSingleThreadAlgorithm extends AlgorithmRunnable {

/*
    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb, int highestNodeId, AlgorithmType type) {
        super(gdb, highestNodeId);
    }

    @Override
    public void compute() {

    }
*/
    public enum AlgorithmType{
        WEAK,
        STRONG
    }


    private int componentID;
    private AlgorithmType myType;

    private Map<Node,TarjanNode> nodeDictionary;
    private Stack<Node> stack;
    private int maxdfs=0;

    public static Set<Node> allNodes;


    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb, int highestNodeId
            , AlgorithmType type, boolean output){
        super(gdb, highestNodeId, output);

        this.myType = type;

        allNodes = new HashSet<Node>(highestNodeId);

        if(myType==AlgorithmType.STRONG){

            this.stack = new Stack<Node>();
            this.nodeDictionary = new HashMap<Node,TarjanNode>();

            // initialize nodeDictionary for tarjans algo
            try (Transaction tx = graphDb.beginTx()) {
                GlobalGraphOperations ggop = GlobalGraphOperations.at(gdb);
                ggop.getAllNodes().iterator();

                ResourceIterator<Node> it = ggop.getAllNodes().iterator();
                while(it.hasNext()){
                    Node n = it.next();
                    allNodes.add(n);
                    nodeDictionary.put(n,new TarjanNode(n));
                }
                it.close();
                tx.success();

            }

        } else{
            // TODO Refactor this!
            try (Transaction tx = graphDb.beginTx()) {
                GlobalGraphOperations ggop = GlobalGraphOperations.at(gdb);
                ggop.getAllNodes().iterator();

                ResourceIterator<Node> it = ggop.getAllNodes().iterator();
                while(it.hasNext()) {
                    Node n = it.next();
                    allNodes.add(n);
                }
                it.close();
                tx.success();

            }

        }

    }


    @Override
    public void compute() {

        timer.start();

        componentID = 1;

        try (Transaction tx = graphDb.beginTx()) {
            // GlobalGraphOperations operations = GlobalGraphOperations.at(graphDb);
            // ResourceIterable<Node> it = operations.getAllNodes();
            Iterator<Node> it = allNodes.iterator();
            while(it.hasNext()){
                // Every node has to be marked as (part of) a component
                it = allNodes.iterator();

                try {
                    Node n = it.next();

                    if(myType==AlgorithmType.WEAK){
                       // try (Transaction tx = graphDb.beginTx()) {
                            new DFS(n, componentID).go(100);  // just to try it
                            componentID++;
                            //System.out.println(allNodes.size());
                       //     tx.success();
                     //   }

                    } else{
                        // System.out.println(allNodes.size());  // just for me TODO Remove!
                        tarjan(n);
                    }

                }catch (NoSuchElementException e){
                    break;
                }

            }

            tx.success();

        }

        timer.stop();


    }

    private void tarjan(Node currentNode){

        TarjanNode v = nodeDictionary.get(currentNode);
        v.dfs = maxdfs;
        v.lowlink = maxdfs;
        maxdfs++;

        v.onStack = true;           // This should be atomic
        stack.push(currentNode);        // !

        //itN.remove();
        allNodes.remove(currentNode);

        Iterable<Relationship> it = currentNode.getRelationships(Direction.OUTGOING);
        for(Relationship r: it){
            Node n_new = r.getOtherNode(currentNode);
            TarjanNode v_new = nodeDictionary.get(n_new);

            if(allNodes.contains(n_new)){
                tarjan(n_new);

                v.lowlink = Math.min(v.lowlink,v_new.lowlink);

            } else if(v_new.onStack){       // O(1)

                v.lowlink = Math.min(v.lowlink,v_new.dfs);
            }

        }

        if(v.lowlink == v.dfs){
            // Root of a SCC

            while(true){
                Node node_v = stack.pop();                      // This should be atomic
                TarjanNode v_new = nodeDictionary.get(node_v);  // !
                v_new.onStack= false;                           // !

                StartComparison.resultCounter.put(node_v.getId(), new AtomicInteger(componentID));
                if(node_v.getId()== currentNode.getId()){
                    componentID++;
                    break;
                }

            }
        }

    }


    public String getResults(){

        Map<Integer, List<Long>> myResults = new TreeMap<Integer,List<Long>>();

        // to adapt to the "old" structure of componentsMap

        for(Long n: StartComparison.resultCounter.keySet()){
            if(!myResults.containsKey(StartComparison.resultCounter.get(n).intValue())){
                ArrayList<Long> newList = new ArrayList<>();
                newList.add(n);
                myResults.put(StartComparison.resultCounter.get(n).intValue(),newList);
            } else{
                List<Long> oldList = myResults.get(StartComparison.resultCounter.get(n).intValue());
                oldList.add(n);
                myResults.put(StartComparison.resultCounter.get(n).intValue(),oldList);
            }
        }

        // Building the result string

        StringBuilder returnString = new StringBuilder();
        returnString.append("Component count: " + myResults.keySet().size() + "\n");
        returnString.append("Components with Size >4\n");
        returnString.append("- - - - - - - -\n");
        for(Integer s:myResults.keySet()){
            if(myResults.get(s).size()<=5){
                continue;
            }

            boolean first = true;
            returnString.append("Component " + s + ": ");
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
        returnString.append("Done in: " + timer.elapsed(TimeUnit.MICROSECONDS)+ "\u00B5s");

        return returnString.toString();
    }

}

