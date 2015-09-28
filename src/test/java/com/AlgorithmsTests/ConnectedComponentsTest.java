package com.AlgorithmsTests;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.ConnectedComponentsSingleThreadAlgorithm;
import de.saschapeukert.StartComparison;
import org.apache.commons.lang3.SystemUtils;
import org.junit.*;

import java.util.*;

/**
 * Created by Sascha Peukert on 27.09.2015.
 */
public class ConnectedComponentsTest {

    private static final String[] argsWCC = {"WCC", "1001000", "1", "8", "1", "true", "WeaklyConnectedComponentTest",
            "1G", "testDB\\graph.db", "Write"};
    private static final String[] argsSCC = {"SCC", "1001000", "1", "8", "1", "true", "StronglyConnectedComponentTest",
            "1G", "testDB\\graph.db", "Write"};
    // don't realy need to write here

    @BeforeClass
    public static void oneTimeSetUp() {

        if(SystemUtils.IS_OS_UNIX){
            argsWCC[8] = "testDB/graph.db";
            argsSCC[8] = "testDB/graph.db";
        }
    }
    @AfterClass
    public static void oneTimeTearDown() {

    }

    @Before
    public void setUp() {

    }

    @Test
    public void WeaklyConnectedComponentsShallNotCrash() {

        try{
            StartComparison.main(argsWCC);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail("It crashed. Why?");

        }
    }

    @Test
    public void WeaklyConnectedComponentsShouldBeCorrect() {

        // Do the Run, get results
        StartComparison.main(argsWCC);
        Map<Integer,List<Long>> resultOfRun = ConnectedComponentsSingleThreadAlgorithm.getMapofComponentToIDs();

        // Check the result
        Assert.assertNull("There should not be an Component with ID 0", resultOfRun.get(0));
        Assert.assertTrue("First Component (A) is wrong",compareValues(resultOfRun,1,new Long[]{0L}));
        Assert.assertTrue("Second Component (B) is wrong",compareValues(resultOfRun,1,new Long[]{1L}));
        Assert.assertTrue("Third Component (CDE) is wrong",compareValues(resultOfRun,3,new Long[]{2L,3L,4L}));
        Assert.assertTrue("Fourth Component (FG) is wrong",compareValues(resultOfRun,2,new Long[]{5L,6L}));
        Assert.assertTrue("Fifth Component (HIJKLMNZ) is wrong",compareValues(resultOfRun,8,new Long[]
                {7L,8L,9L,10L,11L,12L,13L,14L}));

    }

    @Test
    public void StronglyConnectedComponentsShouldBeCorrect() {

        // Do the Run, get results
        StartComparison.main(argsSCC);
        Map<Integer,List<Long>> resultOfRun = ConnectedComponentsSingleThreadAlgorithm.getMapofComponentToIDs();

        // Check the result
        Assert.assertNull("There should not be an Component with ID 0", resultOfRun.get(0));
        Assert.assertTrue("First Component (A) is wrong",compareValues(resultOfRun,1,new Long[]{0L}));
        Assert.assertTrue("Second Component (B) is wrong",compareValues(resultOfRun,1,new Long[]{1L}));
        Assert.assertTrue("Third Component (CDE) is wrong",compareValues(resultOfRun,3,new Long[]{2L,3L,4L}));
        Assert.assertTrue("Fourth Component (FG) is wrong",compareValues(resultOfRun,2,new Long[]{5L,6L}));
        Assert.assertTrue("Fifth Component (HZ) is wrong",compareValues(resultOfRun,2,new Long[]{7L,14L}));
        Assert.assertTrue("Sixth Component (I) is wrong",compareValues(resultOfRun,1,new Long[]{8L}));
        Assert.assertTrue("Seventh Component (J) is wrong",compareValues(resultOfRun,1,new Long[]{9L}));
        Assert.assertTrue("Eighth Component (K) is wrong",compareValues(resultOfRun,1,new Long[]{10L}));
        Assert.assertTrue("Ninth Component (L) is wrong",compareValues(resultOfRun,1,new Long[]{11L}));
        Assert.assertTrue("Tenth Component (M) is wrong",compareValues(resultOfRun,1,new Long[]{12L}));
        Assert.assertTrue("Eleventh Component (N) is wrong",compareValues(resultOfRun,1,new Long[]{13L}));

    }


    private boolean compareValues(Map<Integer,List<Long>> map, int sizeOfList, Long[] valuesOfList){

        for(int key :map.keySet()){
            List<Long> list = map.get(key);
            Collections.sort(list);

            if(list.size()==sizeOfList){

                int i=0; // counts the correct Entrys
                for(Long l:valuesOfList){
                    try {
                        if (list.get(i).equals(l)) {
                            i++;
                        } else {
                            break;
                        }
                    } catch (Exception e){
                        break;
                    }
                }

                if(i==sizeOfList){
                    return true;
                }

            }

        }

        return false;
    }


    @Test
    public void StronglyConnectedComponentsShallNotCrash() {

        try{
            StartComparison.main(argsSCC);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail("It crashed. Why?");

        }
    }
}
