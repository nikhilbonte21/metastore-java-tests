package com.tests.main.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.v1.model.instance.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    public static String toJson(Object obj) {
        String ret;
        try {
            if (obj instanceof JsonNode && ((JsonNode) obj).isTextual()) {
                ret = ((JsonNode) obj).textValue();
            } else {
                ret = mapper.writeValueAsString(obj);
            }
        }catch (IOException e){
            LOG.error("Utils.toJson()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        T ret = null;

        if (jsonStr != null) {
            try {
                ret = mapper.readValue(jsonStr, type);

                if (ret instanceof Struct) {
                    ((Struct) ret).normalize();
                }
            } catch (IOException e) {
                LOG.error("Utils.fromJson()", e);

                ret = null;
            }
        }

        return ret;
    }




}
