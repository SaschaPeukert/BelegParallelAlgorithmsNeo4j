package com.AlgorithmsTests;

import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.OutputTop20;
import de.saschapeukert.Starter;
import org.apache.commons.lang3.SystemUtils;
import org.junit.*;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by Sascha Peukert on 25.09.2015.
 */
public class RandomWalkTest {

    private static final String[] args = {"RW", "1001000", "1", "8", "true", "RandomWalkCounterTest",
            "1G", "testDB\\graph.db", "Write"};

    @BeforeClass
    public static void oneTimeSetUp() {
        if(SystemUtils.IS_OS_UNIX){
            args[8] = "testDB/graph.db";
        }
    }
    @AfterClass
    public static void oneTimeTearDown() {}

    @Before
    public void setUp() {
        Starter.unittest = true;
    }

    @Test
    public void RandomWalkShallNotCrash() {
        try{
            Starter.main(args);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail("It crashed. Why?");
        }
    }

    @Test
    public void Top7NodesShouldHaveFixedPositionAfterRandomWalk() {

        Starter.main(args);

        DBUtils db = DBUtils.getInstance(args[7], args[6]);
        Transaction tx = db.openTransaction();  // NOPE
        List<Object[]> resultList= OutputTop20.getTop20(args[5]);

        assertEquals((resultList.get(0)[0]),14L);
        assertEquals((resultList.get(1)[0]),7L);

        List<Long> checkListTop3_7 = new ArrayList<>();
        for(int i=2;i<=6;i++){
            Long id =(Long)resultList.get(i)[0];
            if(checkListTop3_7.contains(id)){
                // That case should never happen
                continue;
            }
            if(id.equals(2L)){
                checkListTop3_7.add(id);
            }
            if(id.equals(3L)){
                checkListTop3_7.add(id);
            }
            if(id.equals(4L)){
                checkListTop3_7.add(id);
            }
            if(id.equals(5L)){
                checkListTop3_7.add(id);
            }
            if(id.equals(6L)){
                checkListTop3_7.add(id);
            }
        }
        assertEquals(checkListTop3_7.size(),5);
        db.closeTransactionWithSuccess(tx);
    }
}

