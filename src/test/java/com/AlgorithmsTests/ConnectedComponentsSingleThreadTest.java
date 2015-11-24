package com.AlgorithmsTests;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.newSTConnectedComponentsAlgo;
import de.saschapeukert.newStartComparison;
import org.apache.commons.lang3.SystemUtils;
import org.junit.*;

import java.util.List;
import java.util.Map;

/**
 * Created by Sascha Peukert on 27.09.2015.
 */
public class ConnectedComponentsSingleThreadTest {

    private static final String[] argsWCC = {"WCC", "1001000", "1", "1", "true", "WeaklyConnectedComponentTest",
            "1G", "testDB\\graph.db", "NoWrite"};
    private static final String[] argsSCC = {"SCC", "1001000", "1", "1", "true", "StronglyConnectedComponentTest",
            "1G", "testDB\\graph.db", "NoWrite"};
    // don't realy need to write here

    @BeforeClass
    public static void oneTimeSetUp() {
        if(SystemUtils.IS_OS_UNIX){
            argsWCC[7] = "testDB/graph.db";
            argsSCC[7] = "testDB/graph.db";
        }
    }

    @AfterClass
    public static void oneTimeTearDown() {}

    @Before
    public void setUp() {}

    @Test
    public void WeaklyConnectedComponentsShallNotCrash() {
        try{
            newStartComparison.main(argsWCC);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail("It crashed. Why?");
        }
    }

    @Test
    public void WeaklyConnectedComponentsShouldBeCorrect() {
        // Do the Run, get results
        newStartComparison.main(argsWCC);
        Map<Integer,List<Long>> resultOfRun = newSTConnectedComponentsAlgo.getMapofComponentToIDs();

        // Check the result
        Assert.assertNull("There should not be an Component with ID 0", resultOfRun.get(0));
        Assert.assertTrue("First Component (A) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{0L}));
        Assert.assertTrue("Second Component (B) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{1L}));
        Assert.assertTrue("Third Component (CDE) is wrong",CompareLists.compareValues(resultOfRun, 3, new Long[]{2L, 3L, 4L}));
        Assert.assertTrue("Fourth Component (FG) is wrong",CompareLists.compareValues(resultOfRun, 2, new Long[]{5L, 6L}));
        Assert.assertTrue("Fifth Component (HIJKLMNZ) is wrong",CompareLists.compareValues(resultOfRun, 8, new Long[]
                {7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L}));
    }

    @Test
    public void StronglyConnectedComponentsShouldBeCorrect() {
        // Do the Run, get results
        newStartComparison.main(argsSCC);
        Map<Integer,List<Long>> resultOfRun = newSTConnectedComponentsAlgo.getMapofComponentToIDs();

        // Check the result
        Assert.assertNull("There should not be an Component with ID 0", resultOfRun.get(0));
        Assert.assertTrue("First Component (A) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{0L}));
        Assert.assertTrue("Second Component (B) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{1L}));
        Assert.assertTrue("Third Component (CDE) is wrong",CompareLists.compareValues(resultOfRun, 3, new Long[]{2L, 3L, 4L}));
        Assert.assertTrue("Fourth Component (FG) is wrong",CompareLists.compareValues(resultOfRun, 2, new Long[]{5L, 6L}));
        Assert.assertTrue("Fifth Component (HZ) is wrong",CompareLists.compareValues(resultOfRun, 2, new Long[]{7L, 14L}));
        Assert.assertTrue("Sixth Component (I) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{8L}));
        Assert.assertTrue("Seventh Component (J) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{9L}));
        Assert.assertTrue("Eighth Component (K) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{10L}));
        Assert.assertTrue("Ninth Component (L) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{11L}));
        Assert.assertTrue("Tenth Component (M) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{12L}));
        Assert.assertTrue("Eleventh Component (N) is wrong",CompareLists.compareValues(resultOfRun, 1, new Long[]{13L}));
    }

    @Test
    public void StronglyConnectedComponentsShallNotCrash() {
        try{
            newStartComparison.main(argsSCC);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail("It crashed. Why?");
        }
    }
}
