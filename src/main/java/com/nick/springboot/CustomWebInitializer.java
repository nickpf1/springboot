package com.nick.springboot;

import org.duckdb.DuckDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

@Component
public class CustomWebInitializer implements ApplicationRunner {
    private static final Logger LOG =
            LoggerFactory.getLogger(CustomWebInitializer.class);

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info(">>STARTING CONNECTION TO DUCK DB");

        Class.forName("org.duckdb.DuckDBDriver");
        Properties props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));

        Connection conn = DriverManager.getConnection("jdbc:duckdb:src/main/resources/my-db", props);
        Statement stmt = conn.createStatement();
//        stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
        // insert two items into the table
//        stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");
        stmt.execute("CREATE TABLE IF NOT EXISTS test_parquet AS SELECT * FROM read_parquet('src/main/resources/table.parquet');");

        try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_parquet")) {
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    LOG.info(metaData.getColumnName(i) + ": " + rs.getString(i));
                }
            }
        }
        stmt.close();
        conn.close();
    }
}
