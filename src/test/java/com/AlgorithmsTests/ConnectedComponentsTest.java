package com.AlgorithmsTests;

import de.saschapeukert.StartComparison;
import org.junit.*;

/**
 * Created by Sascha Peukert on 27.09.2015.
 */
public class ConnectedComponentsTest {

    private static String[] argsWCC = {"WCC", "1001000", "1", "8", "1", "true", "WeaklyConnectedComponentTest",
            "1G", "testDB\\graph.db", "Write"};
    private static String[] argsSCC = {"SCC", "1001000", "1", "8", "1", "true", "StronglyConnectedComponentTest",
            "1G", "testDB\\graph.db", "Write"};


    @BeforeClass
    public static void oneTimeSetUp() {


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
    public void StronglyConnectedComponentsShallNotCrash() {

        try{
            StartComparison.main(argsSCC);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail("It crashed. Why?");

        }
    }
}
