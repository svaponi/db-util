package io.github.svaponi.dbutil;

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