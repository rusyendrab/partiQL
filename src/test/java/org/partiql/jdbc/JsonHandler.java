package org.partiql.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class JsonHandler {
    private static final long serialVersionUID = 1L;

    private transient  ObjectMapper objectMapper = new ObjectMapper();
    public List<Map> parseJson(String json) throws IOException {
        String cleanJson = json.replace("<<", "[").replace(">>","]").replace("'","\"");
         return objectMapper.readValue(cleanJson, new TypeReference<>(){});
    }

}
