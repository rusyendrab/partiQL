package org.partiql.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class Test {
    public static void main(String[] args) throws SQLException {
        Connection connection;
        Logger logger = Logger.getLogger("org.partiql.jdbc");
        try {
            Class.forName("org.partiql.jdbc.PartiQLDriver");
            //connection = DriverManager.getConnection("jdbc:partiql:src/test/resources/ion/tutorial-all-data.env");
            //connection = DriverManager.getConnection("jdbc:partiql:src/test/resources/ion/invoice.json");
            //connection = DriverManager.getConnection("jdbc:partiql:src/test/resources/ion/test.json");
            //connection = DriverManager.getConnection("jdbc:partiql:src/test/resources/ion/test1.json");
            connection = DriverManager.getConnection("jdbc:partiql:src/test/resources/ion/invoice.json");
            PartiQLStatement statement = (PartiQLStatement) connection.createStatement();
            String query = "SELECT e.id, \n" +
                    "       e.name AS employeeName, \n" +
                    "       e.title AS title\n" +
                    "FROM hr.employees e\n" ;
                    //"WHERE e.title = 'Dev Mgr'\n";
            String query1 = "SELECT e.id, e.name, e.title from employees e " ;
            String query2 = "SELECT e.InvoiceNumber,e.CreatedTime,e.StoreID from invoice e" ;
            ResultSet results = statement.executeQuery(query2);
            do{
                results.getLong("CreatedTime");
                //p("InvoiceNumber: " + results.getString("InvoiceNumber"));
                p("CreatedTime: " + results.getLong("CreatedTime"));
                //p("StoreID: " + results.getString("StoreID"));
                //p(results.getString("name"));
                //p(results.getString("title"));
                //p(results.getString("InvoiceNumber"));
            }while(results.next());



        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
            logger.info("Exception Occurred.");
        }
    }
    private static void p(String s){
        System.out.println(s);
    }
}
