package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Datastructures.TarjanNode;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 17.10.2015.
 */
public abstract class AbstractConnectedComponents extends MyAlgorithmBaseRunnable {

    protected int componentID=1;
    protected final CCAlgorithmType myType;

    protected Map<Long,TarjanNode> nodeDictionary;
    protected Stack<Long> stack;
    protected int maxdfs=0;

    public static Set<Long> allNodes; // except the trivial CCs

    protected AbstractConnectedComponents(CCAlgorithmType type, boolean output) {
        super(output);
        this.myType = type;

        if(myType== CCAlgorithmType.STRONG) {
            // initialize nodeDictionary for tarjans algo
            this.stack = new Stack<>();
            this.nodeDictionary = new HashMap<>(db.highestNodeKey);
        }

        prepareAllNodes();
    }

    @Override
    public void compute() {

        timer.start();
        Iterator<Long> it = allNodes.iterator();

        while(it.hasNext()){
            // Every node has to be marked as (part of) a component
            it = allNodes.iterator();

            try {
                Long n = it.next();
                if(myType== CCAlgorithmType.WEAK){

                    weakly(n, componentID);
                    componentID++;

                } else{
                    strongly(n);
                }

            }catch (NoSuchElementException e){
                break;
            }

        }

        timer.stop();

    }

    protected abstract void weakly(Long n, int id);
    protected abstract void strongly(Long n);


    private void prepareAllNodes(){
        allNodes = new HashSet<>(db.highestNodeKey);

        tx = db.openTransaction();

        ResourceIterator<Node> it = db.getIteratorForAllNodes();
        while(it.hasNext()){
            Node n = it.next();

            trimOrAddToAllNodes(n);

            if(myType== CCAlgorithmType.STRONG)
                nodeDictionary.put(n.getId(),new TarjanNode());

        }

        it.close();
        db.closeTransactionWithSuccess(tx);
    }

    protected void trimOrAddToAllNodes(Node n){
        if(n.getDegree()==0){
            // trivial CC
            StartComparison.putIntoResultCounter(n.getId(), new AtomicInteger(componentID));
            componentID++;

        } else{
            allNodes.add(n.getId());
        }
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
