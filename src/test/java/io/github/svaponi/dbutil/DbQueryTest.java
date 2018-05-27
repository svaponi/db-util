package io.github.svaponi.dbutil;

import lombok.Data;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {SpringConfig.class})
@TestPropertySource("classpath:h2.properties")
public class DbQueryTest {

    static final double _delta = 0.01;
    static final boolean _dropAllAtTheEnd = false;
    private static final String SQL_TIMESTAMP_FORMAT = "YYYYMMDD HH24:MI:SS.FF";
    private static final SimpleDateFormat ts = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
    private static DbQuery _dbq;
    final int NUM_OF_COLUMNS = 5;
    // final Date C0 = new Date(); // we could use Date if we didn't care about milliseconds, but consider that date objects coming back from db are java.sql.Timestamp
    final Timestamp C0 = new Timestamp(new Date().getTime());
    final String C1 = "foo";
    final int C2 = 1;
    final long C3 = Long.MAX_VALUE;
    final double C4 = 0.23;
    final int BATCH_LENGTH = 5;

    @Autowired
    private DbQuery dbq;

    @AfterClass
    public static void afterClass() {

        if (!_dropAllAtTheEnd) {
            return;
        }

        _dbq.batch(asList("DROP TABLE IF EXISTS test01", "DROP TABLE IF EXISTS test05"));
    }

    @Before
    public void setup() {
        if (_dbq == null) {
            _dbq = dbq;
        }
        Assert.assertNotNull("DbQuery should be not null", dbq);
        dbq.update("DROP TABLE IF EXISTS test01");
        dbq.update("DROP TABLE IF EXISTS test05");
        dbq.update("CREATE TABLE test01 (C0 TIMESTAMP, C1 VARCHAR, C2 INTEGER, C3 BIGINT, C4 DOUBLE)");
        dbq.select("SELECT * FROM test01"); // verifies the table exists
    }

    @Test
    public void test_insertAndSelect() {

        dbq.update(String.format("INSERT INTO test01(C0, C1, C2, C3, C4) SELECT to_timestamp('%s','%s') as C0, '%s' as C1, %s as C2, %s as C3, %s as C4", formatDate(C0), SQL_TIMESTAMP_FORMAT, C1, C2, C3, C4));

        final DbResult result = dbq.select("SELECT C0, C1, C2, C3, C4 FROM test01");

        // Test result is not null
        Assert.assertNotNull(result);

        // Test number rows
        Assert.assertEquals(1, result.size());

        // Test number of columns
        Assert.assertEquals(NUM_OF_COLUMNS, result.columns.size());

        // Test column names
        for (int i = 0; i < NUM_OF_COLUMNS; i++) {
            Assert.assertEquals("C" + i, result.columns.get(i));
        }

        // Test values
        Assert.assertEquals(addDays(C0, 0), result.get(0, "C0"));
        Assert.assertEquals(formatDate(addDays(C0, 0)), formatDate(result.get(0, "C0")));
        Assert.assertEquals(C1, result.get(0, "C1"));
        Assert.assertEquals(C2, result.get(0, "C2"));
        Assert.assertEquals(C3, result.get(0, "C3"));
        Assert.assertEquals((double) C4, (double) result.get(0, "C4"), _delta);
    }

    @Test
    public void test_queryDelete() {

        dbq.update("INSERT INTO test01(C0, C1, C2, C3, C4) VALUES (?, ?, ?, ?, ?)", C0, C1, C2, C3, C4);

        final long count = dbq.count("SELECT count(*) FROM test01");
        Assert.assertEquals(1l, count);

        final int n = dbq.update("DELETE FROM test01");

        // Test number of deleted rows
        Assert.assertEquals(1, n);
    }

    @Test
    public void test_multipleInserts() {

        int i = 0;

        dbq.update("INSERT INTO test01 (C0, C1, C2, C3, C4) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
                C0, C1, C2, C3, C4,
                addDays(C0, ++i), C1 + i, C2 + i, C3 - i, C4 + i,
                addDays(C0, ++i), C1 + i, C2 + i, C3 - i, C4 + i,
                addDays(C0, ++i), C1 + i, C2 + i, C3 - i, C4 + i,
                addDays(C0, ++i), C1 + i, C2 + i, C3 - i, C4 + i
        );

        final long count = dbq.count("SELECT count(*) FROM test01");
        Assert.assertEquals(5l, count);

        final DbResult result = dbq.select("SELECT * FROM test01 ORDER BY C0");

        // Test number of rows
        Assert.assertEquals("DbResult should have 2 rows", 5, result.size());

        i = 0;

        // Test 1st row values
        Assert.assertEquals(C0, result.get(i, "C0"));
        Assert.assertEquals(formatDate(C0), formatDate(result.get(i, "C0")));
        Assert.assertEquals(C1, result.get(i, "C1"));
        Assert.assertEquals(C2, result.get(i, "C2"));
        Assert.assertEquals(C3, result.get(i, "C3"));
        Assert.assertEquals(C4, result.get(i, "C4"));

        i++;

        // Test 2nd row values
        Assert.assertEquals(addDays(C0, i), result.get(i, "C0"));
        Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(result.get(i, "C0")));
        Assert.assertEquals(C1 + i, result.get(i, "C1"));
        Assert.assertEquals(C2 + i, result.get(i, "C2"));
        Assert.assertEquals(C3 - i, result.get(i, "C3"));
        Assert.assertEquals(C4 + i, result.get(i, "C4"));

        i++;

        // Test 3rd row values
        Assert.assertEquals(addDays(C0, i), result.get(i, "C0"));
        Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(result.get(i, "C0")));
        Assert.assertEquals(C1 + i, result.get(i, "C1"));
        Assert.assertEquals(C2 + i, result.get(i, "C2"));
        Assert.assertEquals(C3 - i, result.get(i, "C3"));
        Assert.assertEquals(C4 + i, result.get(i, "C4"));

        i++;

        // Test 4th row values
        Assert.assertEquals(addDays(C0, i), result.get(i, "C0"));
        Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(result.get(i, "C0")));
        Assert.assertEquals(C1 + i, result.get(i, "C1"));
        Assert.assertEquals(C2 + i, result.get(i, "C2"));
        Assert.assertEquals(C3 - i, result.get(i, "C3"));
        Assert.assertEquals(C4 + i, result.get(i, "C4"));

        i++;

        // Test 5th row values
        Assert.assertEquals(addDays(C0, i), result.get(i, "C0"));
        Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(result.get(i, "C0")));
        Assert.assertEquals(C1 + i, result.get(i, "C1"));
        Assert.assertEquals(C2 + i, result.get(i, "C2"));
        Assert.assertEquals(C3 - i, result.get(i, "C3"));
        Assert.assertEquals(C4 + i, result.get(i, "C4"));
    }

    @Test
    public void test_batch() {

        final List<String> queries = new ArrayList<>();

        queries.add("CREATE TABLE test05 (C0 TIMESTAMP, C1 VARCHAR, C2 INTEGER, C3 BIGINT, C4 DOUBLE)");

        for (int i = 0; i < BATCH_LENGTH; i++) {
            final Date d = addDays(C0, i);
            queries.add(String.format("INSERT INTO test05 (C0, C1, C2, C3, C4) VALUES (to_timestamp('%s','%s'), '%s', %s, %s, %s)", formatDate(d), SQL_TIMESTAMP_FORMAT, (C1 + i), (C2 + i), (C3 - i), (C4 + i)));
        }

        dbq.batch(queries);

        final DbResult result = dbq.select("SELECT * FROM test05");
        for (int i = 0; i < BATCH_LENGTH; i++) {
            Assert.assertEquals(addDays(C0, i), result.get(i, "C0"));
            Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(result.get(i, "C0")));
            Assert.assertEquals(C1 + i, result.get(i, "C1"));
            Assert.assertEquals(C2 + i, result.get(i, "C2"));
            Assert.assertEquals(C3 - i, result.get(i, "C3"));
            Assert.assertEquals((double) C4 + i, (double) result.get(i, "C4"), _delta);
        }
    }

    @Test
    public void test_getEntityList() {

        final List<String> queries = IntStream.range(0, 2)
                .mapToObj(i -> String.format("INSERT INTO test01 (C0, C1, C2, C3, C4) VALUES (to_timestamp('%s','%s'), '%s', %s, %s, %s)", formatDate(addDays(C0, i)), SQL_TIMESTAMP_FORMAT, (C1 + i), (C2 + i), (C3 - i), (C4 + i)))
                .collect(Collectors.toList());

        dbq.batch(queries);

        final List<Test01> entities = dbq.getEntityList("SELECT * FROM test01 ORDER BY C0", Test01.class);

        // Test number of rows
        Assert.assertEquals("EntityList should have 2 items", 2, entities.size());

        int i = 0;

        // Test 1st row values
        Test01 entity = entities.get(0);
        Assert.assertEquals(addDays(C0, i), entity.getC0());
        Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(entity.getC0()));
        Assert.assertEquals(C1 + i, entity.getC1());
        Assert.assertEquals(C2 + i, entity.getC2());
        Assert.assertEquals(C3 - i, entity.getC3());
        Assert.assertEquals(C4 + i, entity.getC4(), _delta);

        i++;

        // Test 2nd row values
        entity = entities.get(1);
        Assert.assertEquals(addDays(C0, i), entity.getC0());
        Assert.assertEquals(formatDate(addDays(C0, i)), formatDate(entity.getC0()));
        Assert.assertEquals(C1 + i, entity.getC1());
        Assert.assertEquals(C2 + i, entity.getC2());
        Assert.assertEquals(C3 - i, entity.getC3());
        Assert.assertEquals(C4 + i, entity.getC4(), _delta);
    }

    private String formatDate(final Object date) {
        if (date instanceof Date) {
            return ts.format(date);
        }
        throw new IllegalArgumentException("not a date");
    }

    private Timestamp addDays(final Timestamp fromDate, final int deltaDays) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(fromDate);
        cal.add(Calendar.DAY_OF_YEAR, deltaDays);
        return new Timestamp(cal.getTime().getTime());
    }

    @Data
    public static class Test01 {
        private Timestamp C0 = new Timestamp(new Date().getTime());
        private String C1 = "foo";
        private int C2 = 1;
        private long C3 = Long.MAX_VALUE;
        private double C4 = 0.23;
    }
}
