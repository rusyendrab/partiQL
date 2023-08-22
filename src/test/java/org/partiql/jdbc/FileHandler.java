package org.partiql.jdbc;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.Map;

public class FileHandler {
    public void writeToFile(String fileName, String content, String contentName) throws IOException {
        //String fileName = "src/test/resources/ion/invoiceOutput.json";
        FileWriter fileWriter = null;
        PrintWriter printWriter =null;
        String cleanContent = content.replace("<<", "{'"+ contentName +"':[").replace(">>","]}");
        try {
            fileWriter = new FileWriter(fileName);
            printWriter = new PrintWriter(fileWriter);
            printWriter.print(cleanContent);
            printWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            printWriter.close();
            fileWriter.close();
        }
    }
    public String readFile(String fileName) throws IOException {
        FileInputStream fis = new FileInputStream(fileName);
        String data = IOUtils.toString(fis, "UTF-8").trim();
        return data;
    }

}
