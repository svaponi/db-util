# io.github.svaponi:db-utils

Provides easy-to-use features to access DB without JPA integration.

## How to

Import dependency.

```xml
<dependency>
    <groupId>io.github.svaponi</groupId>
    <artifactId>db-utils</artifactId>
    <version>1.0</version>
</dependency>
```

To build a `DbQuery` object you just need a `javax.sql.DataSource` object to pass as argument to the unique constructor.

Here an example with Spring.

```java
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import javax.sql.DataSource;

@Configuration
public class SpringConfig {

    @Bean
    public DbQuery dbQuery(final DataSource dataSource) {
        return new DbQuery(dataSource);
    }

    @Bean
    public DriverManagerDataSource driverManagerDataSource(
            @Value("${jdbc.driverClassName}") final String driverClassName,
            @Value("${jdbc.url}") final String url,
            @Value("${jdbc.user}") final String username,
            @Value("${jdbc.password}") final String password
    ) {
        final DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setDriverClassName(driverClassName);
        driverManagerDataSource.setUrl(url);
        driverManagerDataSource.setUsername(username);
        driverManagerDataSource.setPassword(password);
        return driverManagerDataSource;
    }
}
```

Then you can start accessing you DB with standard SQL.

```java
public class Foo {

    public void foo() {
        Timestamp C0 = new Timestamp();
        String C1 = "foo";
        int C2 = 1;
        long C3 = Long.MAX_VALUE;
        double C4 = 0.23;
        
        // Create a new table
        dbq.update("CREATE TABLE test01 (" +
        "C0 timestamp, " +
        "C1 varchar, " +
        "C2 integer, " +
        "C3 bigint, " +
        "C4 double)");
        
        // Insert data into table
        dbq.update("INSERT INTO test01(C0, C1, C2, C3, C4) VALUES (?, ?, ?, ?, ?)", C0, C1, C2, C3, C4);

        // Select data from table
        DbResult result = dbq.select("SELECT C0, C1, C2, C3, C4 FROM test01");
        
        // Delete data from table
        int n = dbq.update("DELETE FROM test01");
        
        Assert.assertEquals(1, n);
    }
}
```
And you could go on with all SQL that comes into your mind. You can also inject results into a entity object, like [`Test01`](https://github.com/svaponi/mir-commons/blob/master/mir-commons-db-utils/src/test/java/it/miriade/commons/dbutils/entities/Test01.java).

```java
public class Foo {

    public List<test01> getList() {
        return dbq.getEntityList("SELECT * FROM test01 ORDER BY C0", Test01.class);
    }
}
```

See all this in action in [`DbQueryTest`](src/test/java/it/miriade/commons/dbutils/DbQueryTest.java)
