package org.partiql.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;

public class ParseUtil {
    public List<String> mapToString(List<Map> rowList){

        List<String> rowStr = new ArrayList<>();
        for(Map map: rowList){
            List<String> rowS = new ArrayList<>();
            Iterator<String> iterator = map.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                String kv = key + ": " + map.get(key);
                rowS.add(kv);
            }
            String row = rowS.stream().map(Object::toString).collect(joining(", "));
            rowStr.add(row);
        }
        return rowStr;
    }

}
