package org.partiql.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.partiql.jdbc.LoadSchema.DataType;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class PartiqlTest {
    public static void main(String[] args) throws SQLException {
        Connection connection;
        Logger logger = Logger.getLogger("org.partiql.jdbc");
        try {
            Map<String, Object> schema =  new LoadSchema().loadSchema("ion/invoice.yaml");
            Class.forName("org.partiql.jdbc.PartiQLDriver");
            connection = DriverManager.getConnection("jdbc:partiql:src/test/resources/ion/invoice.json");
            PartiQLStatement statement = (PartiQLStatement) connection.createStatement();

            String query1 = "SELECT e.InvoiceNumber, e.CreatedTime, e.StoreID, e.PosID, e.CashierID, e.CustomerType, e.CustomerCardNo, " +
                    " e.TotalAmount, e.NumberOfItems, e.PaymentMethod, e.TaxableAmount, e.CGST, e.SGST, e.CGST, e.CESS, e.CGST, e.DeliveryType, " +
                    "e.DeliveryAddress " +
                    " from invoice e" ;
            String query2 = "SELECT e.InvoiceNumber, e.CreatedTime, e.StoreID, e.PosID, e.CashierID, e.CustomerType, e.CustomerCardNo," +
                    " e.TotalAmount, e.NumberOfItems, e.PaymentMethod, e.TaxableAmount, e.CGST, e.SGST, e.CGST, e.CESS, e.CGST, e.DeliveryType, " +
                    "a.AddressLine as DeliveryAddress_AddressLine, a.City as DeliveryAddress_City, a.State as DeliveryAddress_State, " +
                    "a.PinCode as DeliveryAddress_PinCode, a.ContactNumber as DeliveryAddress_ContactNumber " +
                    " from invoice e LEFT CROSS JOIN e.DeliveryAddress as a" ;
            String query = "SELECT e.InvoiceNumber, e.CreatedTime, e.StoreID, e.PosID, e.CashierID, e.CustomerType, e.CustomerCardNo," +
                    " e.TotalAmount, e.NumberOfItems, e.PaymentMethod, e.TaxableAmount, e.CGST, e.SGST, e.CGST, e.CESS, e.CGST, e.DeliveryType, " +
                    " a.AddressLine as DeliveryAddress_AddressLine, a.City as DeliveryAddress_City, a.State as DeliveryAddress_State, " +
                    " a.PinCode as DeliveryAddress_PinCode, a.ContactNumber as DeliveryAddress_ContactNumber, " +
                    " li.ItemCode as InvoiceLineItems_ItemCode, li.ItemDescription as InvoiceLineItems_ItemDescription, li.ItemPrice as InvoiceLineItems_ItemPrice, li.ItemQty as InvoiceLineItems_ItemQty, li.TotalValue as InvoiceLineItems_TotalValue " +
                    " from invoice e " +
                    " LEFT CROSS JOIN e.DeliveryAddress as a " +
                    " LEFT CROSS JOIN e.InvoiceLineItems as li" ;

            ResultSet results = statement.executeQuery(query);
            p(results.toString());

            //String[] strArray = results.toString().split("},");
            String fileName = "src/test/resources/ion/invoiceOutput.json";
            FileWriter fileWriter = new FileWriter(fileName);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.print(results.toString());
            printWriter.close();

            List<String> kvList = schema.entrySet().stream().map(kv -> parse(kv,"", new ArrayList<>())).flatMap(List::stream).collect(toList());
            List<Map<String, Object>> res = new ArrayList<>();
            do{
                res = kvList.stream().map(kv -> {
                    try {
                        return getValue(results, kv);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

            }while(results.next());
            String flattenedRow = res.stream()
                    //.map(m-> m.values())
                    .map(Object::toString).collect(joining(", "));
            System.out.println(flattenedRow);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException | IOException e) {
            e.printStackTrace();
            logger.info("Exception Occurred.");
        }
    }
    private static void p(String s){
        System.out.println(s);
    }
    private static List<String> parse(Map.Entry<String, Object> kv, String acc, List<String> accList){
        //List<String> lst = new ArrayList<>();
        String key ="";
        if(acc!=null && acc.length() > 0){
            key = acc;
        }else {
            key = kv.getKey();
        }
        if(key.contains("DeliveryAddress")){
            //System.out.println("test");
        }
        if(kv.getValue() instanceof java.util.LinkedHashMap){
            StringBuffer buffer = new StringBuffer();
            buffer.append(kv.getKey());
            ((LinkedHashMap<String, Object>) kv.getValue()).entrySet().stream().peek(aa-> System.out.println(aa.getKey() + ": " + aa.getValue())).map(kk ->
                    parse(kk,buffer.toString() + "_" + kk.getKey(), accList)
            ).collect(toList());
        }else if (kv.getValue() instanceof java.lang.String){
            String str = key + "," + kv.getValue();
            if(kv.getValue()!=null && ((String) kv.getValue()).length() > 0){
                accList.add(str);
            }
        }

        return accList;
    }

    private static Map<String, Object> getValue(ResultSet results, String kv) throws SQLException {
        if(kv.contains("DeliveryAddress")){
            //System.out.println("test");
        }
        String[] keyVal = kv.split(",");
        String fieldName = keyVal[0];
        String dt = keyVal[1];
        DataType dataType = DataType.valueOf(dt);
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
