package org.partiql.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class PartiQLUtil {
    public Connection getConnectionToJsonFile(String filePath) {
        Connection connection = null;
        Logger logger = Logger.getLogger("org.partiql.jdbc");
        try {
            Class.forName("org.partiql.jdbc.PartiQLDriver");
            connection = DriverManager.getConnection("jdbc:partiql:" + filePath);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
            logger.info("Exception Occurred while getting the PartiQL connection");
        }
        return connection;
    }
    public PartiQLStatement getStatementToJsonFile(String filePath) throws SQLException {
        PartiQLStatement statement = (PartiQLStatement) getConnectionToJsonFile(filePath).createStatement();
        return statement;
    }
    public ResultSet executeQuery(String filePath, String query) throws SQLException {
        ResultSet results = getStatementToJsonFile(filePath).executeQuery(query);
        return results;
    }
    public String executeQueryAndPrintResult(String filePath, String query, String schemaFile, boolean bothKeyAndValues) throws SQLException {
        ResultSet results = getStatementToJsonFile(filePath).executeQuery(query);
        List<Map<String, Object>> res = new ArrayList<>();
        String flattenedRow ="";
        try {
            List<String> kvList = new LoadSchema().getFlattenedKV(schemaFile);
            do{
                res = kvList.stream().map(kv -> {
                    try {
                        return getValue(results, kv);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

            }while(results.next());

            if(bothKeyAndValues){
                flattenedRow = res.stream().map(Object::toString).collect(joining(", "));
            }else{
                flattenedRow = res.stream().map(m-> m.values()).map(Object::toString).collect(joining(", "));
            }
            System.out.println(flattenedRow);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return flattenedRow;
    }
    private static Map<String, Object> getValue(ResultSet results, String kv) throws SQLException {
        if(kv.contains("DeliveryAddress")){
            //System.out.println("test");
        }
        String[] keyVal = kv.split(",");
        String fieldName = keyVal[0];
        String dt = keyVal[1];
        LoadSchema.DataType dataType = LoadSchema.DataType.valueOf(dt);
        Map<String, Object> map = new HashMap<>();
        switch (dataType){
            case String:
                map.put(fieldName, results.getString(fieldName));
                break;
            case Long:
                results.getLong(fieldName);
                map.put(fieldName, results.getLong(fieldName));
                break;
            case Int:
                map.put(fieldName, results.getInt(fieldName));
                break;
            case BigDecimal:
                map.put(fieldName, results.getBigDecimal(fieldName));
                break;
            case Float:
                map.put(fieldName, results.getFloat(fieldName));
                break;
            default:
                map.put(fieldName, results.getString(fieldName));
                break;
        }
        return map;
    }
}
