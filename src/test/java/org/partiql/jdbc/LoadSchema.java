package org.partiql.jdbc;

import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class LoadSchema {
    public enum DataType {
        String, Long, Int, BigDecimal, Float;
    }
    public Map<String, Object> loadSchema(String schemaName)
    {
        Yaml yaml = new Yaml();
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(schemaName);
        Map<String, Object> schema = yaml.load(inputStream);
        return schema;
    }

    public static void main(String[] args) {
        String schema = "ion/invoice.yaml";
        new LoadSchema().loadSchema(schema);
    }
    public List<String> getFlattenedKV(String fileName) throws IOException {
        //"ion/invoice.yaml"
        Map<String, Object> schema =  loadSchema(fileName);
        List<String> kvArray = schema.entrySet().stream().map(kv -> parse(kv,"", new ArrayList<>())).flatMap(List::stream).collect(toList());
        return kvArray;
    }
    private static List<String> parse(Map.Entry<String, Object> kv, String acc, List<String> accList){
        String key ="";
        if(acc!=null && acc.length() > 0){
            key = acc;
        }else {
            key = kv.getKey();
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
}
