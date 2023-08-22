package org.partiql.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.maxBy;

public class Flattner {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger("org.partiql.jdbc.Flattner");
        try {
            String queryFilePath = "src/test/resources/ion/flattenQuery.sql";
            String jsonInputFile = "src/test/resources/ion/invoice.json";
            FileHandler fileHandler = new FileHandler();
            String query = fileHandler.readFile(queryFilePath);
            String flattendedJson = new PartiQLUtil().executeQuery(jsonInputFile, query).toString();
            List<Map> rowList = new JsonHandler().parseJson(flattendedJson);
            List<String> rowStr = new ParseUtil().mapToString(rowList);
            rowStr.forEach(row -> System.out.println("ROW: " + row));
            System.out.println("Flattening Successfully Completed.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            logger.info("Exception Occurred.");
        }
    }

}
