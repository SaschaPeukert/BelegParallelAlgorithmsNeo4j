package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Abst.myAlgorithmBaseCallable;
import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.BFS;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.Datastructures.TarjanInfo;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Sascha Peukert on 17.10.2015.
 */
public class STConnectedComponentsAlgo extends myAlgorithmBaseCallable {

    long componentID= DBUtils.getInstance("","").highestNodeKey+1;
    final CCAlgorithmType myType;

    private Map<Long,TarjanInfo> nodeDictionary;
    private Stack<Long> stack;
    private int maxdfs=0;

    public static Set<Long> allNodes; // except the trivial CCs

    public STConnectedComponentsAlgo(CCAlgorithmType type, TimeUnit timeUnit, boolean output) {
        super(timeUnit, output);
        this.myType = type;

        if(myType== CCAlgorithmType.STRONG) {
            // initialize nodeDictionary for tarjans algo
            this.stack = new Stack<>();
            this.nodeDictionary = new HashMap<>();
        }
    }

    @Override
    protected void compute() {

        timer.start();

        prepareAllNodes();
        if(myType== CCAlgorithmType.WEAK) {
            weakly();
        } else{
            strongly();
        }

        timer.stop();
    }

    void strongly(){

        Iterator<Long> it = allNodes.iterator();
        while(it.hasNext()) {

            try {
                Long n = it.next();
                tarjan(n);

            } catch (NoSuchElementException e) {
                break;
            }
            it = allNodes.iterator();
        }
    }

    void weakly(){

        Iterator<Long> it = allNodes.iterator();
        while(it.hasNext()){
            // Every node has to be marked as (part of) a component

            try {
                Long n = it.next();
                searchForWeakly(n);
                componentID++;

            }catch (NoSuchElementException e){
                break;
            }
            it = allNodes.iterator();
        }
    }

    void searchForWeakly(long n){
        Set<Long> reachableIDs = BFS.go(n, Direction.BOTH);

        registerSCCandRemoveFromAllNodes(reachableIDs,componentID);
    }

    private void prepareAllNodes(){

        allNodes = new HashSet<>();

        ResourceIterator<Node> it = db.getIteratorForAllNodes();
        while(it.hasNext()){
            Node n = it.next();

            trimOrAddToAllNodes(n);

            if(myType== CCAlgorithmType.STRONG)
                nodeDictionary.put(n.getId(),new TarjanInfo());
        }
        it.close();

    }

    private void trimOrAddToAllNodes(Node n){
        if(this.myType==CCAlgorithmType.STRONG){
            if(n.getDegree(Direction.OUTGOING)==0 || n.getDegree(Direction.INCOMING)==0){
                // trivial CC
                StartComparison.putIntoResultCounter(n.getId(), new AtomicLong(componentID));
                componentID++;
            } else{
                allNodes.add(n.getId());
                furtherInspectNodeWhileTrim(n);
            }
        } else{
            if(n.getDegree()==0){
                // trivial CC
                StartComparison.putIntoResultCounter(n.getId(), new AtomicLong(componentID));
                componentID++;
            } else{
                allNodes.add(n.getId());
                furtherInspectNodeWhileTrim(n);
            }
        }
    }

    void furtherInspectNodeWhileTrim(Node n){
        // Overwrite this method to add code for trim()
    }

    public static Map<Integer, List<Long>> getMapofComponentToIDs(){

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

        return myResults;
    }

    public String getResults(){

        Map<Integer, List<Long>> myResults = getMapofComponentToIDs();

        // Building the result string
        StringBuilder returnString = new StringBuilder();
        returnString.append("Component count: ").append(myResults.keySet().size()).append("\n");
        returnString.append("Components with Size between 3 and 5\n");
        returnString.append("- - - - - - - -\n");
        for(Integer s:myResults.keySet()){
            if((myResults.get(s).size()<=2) || (myResults.get(s).size()>=6)){
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


    private void tarjan(Long currentNode){

        TarjanInfo v = nodeDictionary.get(currentNode);
        v.dfs = maxdfs;
        v.lowlink = maxdfs;
        maxdfs++;

        v.onStack = true;           // This should be
        stack.push(currentNode);        // atomic

        allNodes.remove(currentNode);

        Iterable<Long> it = db.getConnectedNodeIDs(db.getReadOperations(), currentNode, Direction.OUTGOING);
        for(Long l:it){

            TarjanInfo v_new = nodeDictionary.get(l);

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
                TarjanInfo v_new = nodeDictionary.get(node_v);  // !
                v_new.onStack= false;                           // !

                StartComparison.putIntoResultCounter(node_v, new AtomicLong(componentID));
                if(Objects.equals(node_v, currentNode)){
                    componentID++;
                    break;
                }
            }
        }
    }

    public static void registerSCCandRemoveFromAllNodes(Set<Long> reachableIDs,long sccID){
        for(Long l:reachableIDs){
            StartComparison.putIntoResultCounter(l, new AtomicLong((sccID)));

        }
        removeFromAllNodes(reachableIDs);
    }

    private static synchronized void removeFromAllNodes(Collection<Long> c){
        allNodes.removeAll(c);
    }
}
