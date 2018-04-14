package com.luogh.learning.lab.demo.geo_fetch_from_baidu;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;

public class JSONUtils {

  /**
   * 对象转json
   */
  public static String objectToJson(final Object data) {
    String jsonString = null;
    try {
      jsonString = JSONObject.toJSONString(data);
      return jsonString;
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }


  public static JSONArray fromList(List<JSONObject> jos) {
    JSONArray ja = new JSONArray();
    for (JSONObject jo : jos) {
      ja.add(jo);
    }
    return ja;
  }
}
