package com.nick.springboot;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@RestController()
@RequestMapping("/v1/parquet")
public class ParquetController {

    public record SchemaResponse(String table, List<Column> column) {}

    public record Column(String name, String type) {}

    public record Query(){}

    @GetMapping("/schema")
    public ResponseEntity<SchemaResponse> schema() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:src/main/resources/my-db");
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select * from test_parquet");
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<Column> columns = new ArrayList<>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columns.add(new Column(metaData.getColumnName(i), metaData.getColumnTypeName(i)));
        }

        return ResponseEntity.ok(new SchemaResponse("test_parquet", columns));
    }

    @GetMapping("")
    public ResponseEntity<Query> query(
            @RequestParam("table") String table,
            @RequestParam("cols") List<String> columns
    ) throws SQLException
    {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:src/main/resources/my-db");
        Statement stmt = conn.createStatement();
        String sql = "select %s from %s".formatted(columns.toString().replace("[", "").replace("]", ""), table);
        System.out.println(sql);

        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.println(metaData.getColumnName(i) + " -> " +rs.getString(i));
            }

        }
        return null;
    }
}
