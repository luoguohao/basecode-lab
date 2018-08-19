package com.luogh.learning.lab.demo.geo_fetch_from_baidu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FetchSpecialDataSingeThread {

  public static void main(String[] args) throws Exception {
    //ReaderResource
    ReaderSpecialResourceSingle reader = new ReaderSpecialResourceSingle("F://ip_3.txt");
    //reader.readResource();

    reader.shufferResource("F://ip_json_new.txt");
  }
}

class ReaderSpecialResourceSingle {

  public static final String ORIGIN_SEPERATOR = "\\s+";
  public static final String SEPERATOR = "\t";
  public static final Pattern IP_MATCH_PATTERN = Pattern
      .compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
  private static final String DUMP_FILE_PATH = "F://ip_special.txt";
  private static final List<String> SPECIAL_AREA = Arrays
      .asList(new String[]{"日本", "朝鲜", "韩国", "老挝", "马来西亚", "越南", "泰国", "印度", "菲律宾"}); //
  private String filePath;

  public ReaderSpecialResourceSingle(String filePath) {
    this.setFilePath(filePath);
  }


  public void readResource() throws Exception {
    File file = new File(filePath);
    if (!file.exists()) {
      throw new Exception("文件不存在！！！");
    }
    BufferedReader in = null;
    PrintWriter w = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
      w = new PrintWriter(DUMP_FILE_PATH, "UTF-8");
      String line = null;
      String[] arr_entry = null;
      ResourceIP resourceIP = null;
      Matcher matcher_ip1 = null;
      Matcher matcher_ip2 = null;
      SpecialData specialData = null;

      while ((line = in.readLine()) != null) {
        //parse record
        try {
          arr_entry = line.split(ORIGIN_SEPERATOR);
          matcher_ip1 = IP_MATCH_PATTERN.matcher(arr_entry[0].trim());
          matcher_ip2 = IP_MATCH_PATTERN.matcher(arr_entry[1].trim());

          if (!SPECIAL_AREA.contains(arr_entry[2].trim())) {
            continue;
          }

          Counters.incrCounter(IpCounter.CURRENT_LINE, 1L);

          if (matcher_ip1.matches() && matcher_ip2.matches()) {

            Counters.incrCounter(IpCounter.MATCHE_RECORD, 1L);
            Counters.incrCounter(IpCounter.TOTAL_RECORD, 1L);

            resourceIP = new ResourceIP(arr_entry[0].trim(), arr_entry[1].trim());
            specialData = new SpecialData(arr_entry[2].trim());
            resourceIP.setJsonData(specialData.getJsonObj());

            if (resourceIP.getJsonData() != null) {
              w.println(resourceIP);
            }
          } else {

            Counters.incrCounter(IpCounter.UN_MATCHE_RECORD, 1L);
            Counters.incrCounter(IpCounter.TOTAL_RECORD, 1L);

            System.out
                .println("unmatch record:" + arr_entry[0].trim() + " , " + arr_entry[1].trim());
            continue;

          }

        } catch (Exception e) {
          Counters.incrCounter(IpCounter.FAILED_RECORD, 1L);
          Counters.printCounter();
          e.printStackTrace();
          continue;
        }
      }
    } finally {
      Counters.printCounter();
      if (in != null) {
        in.close();
      }
      if (w != null) {
        w.close();
      }
      ;
    }

  }


  public void shufferResource(String filePath) throws Exception {
    File file = new File(filePath);
    if (!file.exists()) {
      throw new Exception("文件不存在！！！");
    }
    BufferedReader in = null;
    PrintWriter w = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
      w = new PrintWriter("F://ip_shuffled.txt", "UTF-8");
      String line = null;
      String[] arr_entry = null;

      while ((line = in.readLine()) != null) {
        //parse record
        try {
          arr_entry = line.split(SEPERATOR);
          if (arr_entry[2].contains("\"status\":0")) {
            w.println(line);
          }

        } catch (Exception e) {
          Counters.incrCounter(IpCounter.FAILED_RECORD, 1L);
          Counters.printCounter();
          e.printStackTrace();
          continue;
        }
      }
    } finally {
      Counters.printCounter();
      if (in != null) {
        in.close();
      }
      if (w != null) {
        w.close();
      }
      ;
    }

  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  enum FetcherCounter {
    TOTAL_FETCH_CNT, FAILED_FETCH_CNT, SUCCESS_FETCH_CNT;
  }

  enum IpCounter {
    TOTAL_RECORD, UN_MATCHE_RECORD, MATCHE_RECORD, FAILED_RECORD, CURRENT_LINE;
  }


  class SpecialData {

    private String cityName;
    private String country;
    private String x;
    private String y;
    private String jsonObj;

    public SpecialData(String country) {
      super();
      //"日本","韩国","马来西亚","越南","泰国","印度","菲律宾","老挝"
      this.country = country;
      if ("日本".equals(this.country)) {
        this.cityName = "东京";
        this.x = "129.68695497";
        this.y = "35.76124399";
      } else if ("韩国".equals(this.country)) {
        this.cityName = "首尔";
        this.x = "126.97796919";
        this.y = "37.566535";
      } else if ("马来西亚".equals(this.country)) {
        this.cityName = "吉隆坡";
        this.x = "101.68685499";
        this.y = "3.139003";
      } else if ("越南".equals(this.country)) {
        this.cityName = "河内";
        this.x = "105.83415979";
        this.y = "21.0277644";
      } else if ("泰国".equals(this.country)) {
        this.cityName = "河内";
        this.x = "100.50176510";
        this.y = "13.7563309";
      } else if ("印度".equals(this.country)) {
        this.cityName = "新德里";
        this.x = "77.20902120";
        this.y = "28.6139391";
      } else if ("菲律宾".equals(this.country)) {
        this.cityName = "马尼拉";
        this.x = "120.9842195";
        this.y = "14.5995124";
      } else if ("老挝".equals(this.country)) {
        this.cityName = "万象";
        this.x = "102.6000000";
        this.y = "17.966667";
      } else if ("朝鲜".equals(this.country)) {
        this.cityName = "平壤市";
        this.x = "125.7625241";
        this.y = "39.0392193";
      }
    }

    public String getCityName() {
      return cityName;
    }

    public void setCityName(String cityName) {
      this.cityName = cityName;
    }

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    public String getX() {
      return x;
    }

    public void setX(String x) {
      this.x = x;
    }

    public String getY() {
      return y;
    }

    public void setY(String y) {
      this.y = y;
    }

    public String getJsonObj() {
      //{"address":"CN|广东|None|None|CHINANET|0|0","content":{"address":"广东省","address_detail":{"city":"unkown","city_code":7,"district":"","province":"广东省","street":"","street_number":""},"point":{"x":"113.39481756","y":"23.40800373"}},"status":0}
      StringBuilder sbuilder = new StringBuilder("{\"address\":\"FOREIGN|").append(this.country)
          .append("|None|None|CHINANET|0|0\",\"content\":{\"address\":\"").append(this.country)
          .append("\",\"address_detail\":{\"city\":\"").append(this.cityName)
          .append("\",\"city_code\":7,\"district\":\"\",\"province\":\"").append(this.country)
          .append(
              "\",\"street\":\"\",\"street_number\":\"\"},\"point\":{\"x\":\"113.39481756\",\"y\":\"23.40800373\"}},\"status\":0}");
      this.jsonObj = sbuilder.toString();
      return this.jsonObj;
    }
  }
}


