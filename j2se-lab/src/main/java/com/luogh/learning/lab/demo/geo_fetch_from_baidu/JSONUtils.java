package com.luogh.learning.lab.demo.geo_fetch_from_baidu;

import java.io.IOException;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JSONUtils {

	private static ObjectMapper mapper = new ObjectMapper();

	static{
		mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public static ObjectMapper getMapper() {
		return mapper;
	}

	/**
	 * 对象转json
	 * @param data
	 * @return
	 */
	public static String objectToJson(final Object data) {
		String jsonString = null;
		try {
			jsonString = mapper.writeValueAsString(data);
			return jsonString;
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * json转对象 @param json
	 * @param typeReference
	 * @return
	 */
	public static <T> T jsonToObject(final String json, TypeReference<?> typeReference) {
		try {
			return (T)mapper.readValue(json, typeReference);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * json 转对象 @param jsonNode
	 * @param typeReference
	 * @return
	 */
	public static <T> T jsonToObject(final JsonNode jsonNode, TypeReference<?> typeReference) {
		try {
			return (T)mapper.readValue(jsonNode, typeReference);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static JSONArray fromList(List<JSONObject> jos) {
		JSONArray ja = new JSONArray();
		for(JSONObject jo : jos) {
			ja.add(jo);
		}
		return ja;
	}


}
