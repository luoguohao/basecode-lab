package com.luogh.learning.lab.demo.geo_fetch_from_baidu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.cookie.Cookie;
import org.apache.http.cookie.CookieOrigin;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.cookie.MalformedCookieException;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BestMatchSpecFactory;
import org.apache.http.impl.cookie.BrowserCompatSpec;
import org.apache.http.impl.cookie.BrowserCompatSpecFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

public class FetchDataMultiThread {
	public static void main(String[] args) throws Exception {
		
		BlockingQueue<ResourceIP> sourceIps = new ArrayBlockingQueue<ResourceIP>(1000);
		BlockingQueue<ResourceIP> convertIps = new ArrayBlockingQueue<ResourceIP>(1000);
		
		ReaderResource reader = new ReaderResource("F://ip_3.txt", sourceIps);
		BaiduFetcher fetcher = new BaiduFetcher(sourceIps,convertIps);
		DumpFetchedData dumper = new DumpFetchedData(convertIps);
		
		
		Thread reader_thread = new Thread(reader);
		
		Thread fetcher_thread = null;
		
		for(int i=0;i<60;i++) {
			fetcher_thread = new Thread(fetcher);
			fetcher_thread.start();
		}
		
		Thread dumper_thread = new Thread(dumper);
		
		reader_thread.start();
		
		dumper_thread.start();
	}
}

class DumpFetchedData implements Runnable {
	
	private static final String DUMP_FILE_PATH = "F://ip_json_multithread.txt";
	private  BlockingQueue<ResourceIP> dumpIpList ;
	
	public DumpFetchedData(BlockingQueue<ResourceIP> dumpIpList){
		this.dumpIpList = dumpIpList;
	}
	
	public  BlockingQueue<ResourceIP> getDumpIpList() {
		return this.dumpIpList;
	}

	public void setDumpIpList(BlockingQueue<ResourceIP> dumpIpList) {
		this.dumpIpList = dumpIpList;
	}

	@Override
	public void run() {
		 PrintWriter w = null;
		  try{
			   w = new PrintWriter(DUMP_FILE_PATH);
			   ResourceIP ip = null;
			   while(!Thread.currentThread().isInterrupted()) {
				   ip = dumpIpList.take();
				   if(ip.getJsonData()!=null){
			    		  w.println(ip);
			    	  }
			   }
		  } catch(IOException e){
			  e.printStackTrace();
		  } catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			  if(w!=null) w.close();
		  }
		
	}
	
}
class BaiduFetcher implements Runnable {
	
	private static final String URL = "http://api.map.baidu.com/location/ip?ak=8M6nM3N18ToLxbUURVZe3IEM&coor=bd09ll";
	private  BlockingQueue<ResourceIP> ips ;
	private  BlockingQueue<ResourceIP> convertIps ;
	
	enum FetcherCounter {
		TOTAL_FETCH_CNT,FAILED_FETCH_CNT,SUCCESS_FETCH_CNT;
	}
	
	public  BaiduFetcher(BlockingQueue<ResourceIP> ips,BlockingQueue<ResourceIP>  convertIps){
		this.ips = ips;
		this.convertIps = convertIps;
	}
	
	@Override
	public void run() {
		
		  ResourceIP ip = null;
		  CloseableHttpClient httpclient = null; 
		  HttpGet httpget = null;
		  CloseableHttpResponse response = null;
		  HttpEntity entity = null;
		  String resposeContent = null;
		  StringBuilder url = new StringBuilder(URL);
		  JSONObject jsonObj = null;
		  int statusCode ;
		  BasicCookieStore cookieStore = new BasicCookieStore();
		  CookieSpecProvider easySpecProvider = new CookieSpecProvider() {
								  public CookieSpec create(HttpContext context) {
						
									  return new BrowserCompatSpec() {
											  @Override
											  public void validate(Cookie cookie, CookieOrigin origin)throws MalformedCookieException {
												  	// Oh, I am easy
											  }
									  };
								  }
		  				};
		  				
		  Registry<CookieSpecProvider> r = RegistryBuilder
				  .<CookieSpecProvider> create()
				  .register(CookieSpecs.BEST_MATCH, new BestMatchSpecFactory())
				  .register(CookieSpecs.BROWSER_COMPATIBILITY,
				  new BrowserCompatSpecFactory())
				  .register("easy", easySpecProvider).build();

		  RequestConfig requestConfig = RequestConfig.custom()
		  .setCookieSpec("easy").setSocketTimeout(10000)
		  .setConnectTimeout(10000).build();
		  
		  long beginParseTime;
		  long endParseTime;
		  
		  while(!Thread.currentThread().isInterrupted()) {
			   try {
				ip = ips.take();
				
				 try {  
					  	url = url.append("&ip=").append(ip.getBeginIP());
			        	httpget =  new HttpGet(url.toString());
			        	
			        	httpclient = HttpClients.custom()
			      			  .setDefaultCookieSpecRegistry(r)
			      			  .setDefaultRequestConfig(requestConfig)
			      			  .setDefaultCookieStore(cookieStore)
			      			  .build();
			        	
			        	beginParseTime = System.currentTimeMillis();
			        	
			        	response = httpclient.execute(httpget);
			        	
			            try {  
			            	entity = response.getEntity(); 
			            	statusCode = response.getStatusLine().getStatusCode();
			            	
			            	 endParseTime = System.currentTimeMillis();
			            	 
			                 System.out.println("the BaiduFetcher Thread :"+Thread.currentThread().getName()+" query cost :"+(endParseTime-beginParseTime)+" ms");
			                 
			                if (entity != null&&statusCode == 200) {
			                	
			                	resposeContent = EntityUtils.toString(entity);
			                	
			                	
			                	jsonObj = JSONObject.fromObject(resposeContent);
			                	
			                	JSONObject tmpJsonObj = null;
			                	
			                    if((Integer)jsonObj.get("status") == 0){
			                    	resetJsonValue(jsonObj,"address");
			                    	
			                    	tmpJsonObj = jsonObj.getJSONObject("content");
			                    	resetJsonValue(tmpJsonObj,"address");
			                    	
			                    	tmpJsonObj = jsonObj.getJSONObject("content").getJSONObject("address_detail");
			                    	resetJsonValue(tmpJsonObj,"city");
			                    	resetJsonValue(tmpJsonObj,"province");
			                    	
			                    	
			                    	Counters.incrCounter(FetcherCounter.TOTAL_FETCH_CNT, 1L);
			                    	Counters.incrCounter(FetcherCounter.SUCCESS_FETCH_CNT, 1L);
			                    	ip.setJsonData(jsonObj.toString());
			                    	
			                    } else {
			                    	Counters.incrCounter(FetcherCounter.TOTAL_FETCH_CNT, 1L);
			                    	Counters.incrCounter(FetcherCounter.FAILED_FETCH_CNT, 1L);
			                    	ip.setJsonData(resposeContent);
			                    	System.out.println("the failed fetch ip is :"+ip.getBeginIP()+" and the resposeContent is :"+ resposeContent);  
			                    }
			                    convertIps.put(ip);  //just put it into the coverted_ips
			                }  
			            } finally {  
			                response.close();  
			            }  
			        } catch (ClientProtocolException e) { 
			        	ips.put(ip); // if failed ,put the unexcute data back 
			            e.printStackTrace();  
			        } catch (IOException e) {
			        	ips.put(ip);
			            e.printStackTrace();  
			        } finally { 
			        	Counters.printCounter();
			            // ???????,??????    
			            try {  
			                httpclient.close();  
			            } catch (IOException e) {  
			                e.printStackTrace();  
			            }  
			        }
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			   
			
		   }
	}
	
	private static void resetJsonValue(JSONObject jsonObj,String key){
		jsonObj.element(key,decodeUnicode(jsonObj.getString(key)));
	}
	
	private static String decodeUnicode(String theString){
		if(theString==null || "".equals(theString.trim())) {
			return "unkown";
		}
		char aChar;
		  int len = theString.length();
		  StringBuffer outBuffer = new StringBuffer(len);
		  for (int x = 0; x < len;) {
		   aChar = theString.charAt(x++);
		   if (aChar == '\\') {
		    aChar = theString.charAt(x++);
		    if (aChar == 'u') {
		     int value = 0;
		     for (int i = 0; i < 4; i++) {
		      aChar = theString.charAt(x++);
		      switch (aChar) {
		      case '0':
		      case '1':
		      case '2':
		      case '3':
		      case '4':
		      case '5':
		      case '6':
		      case '7':
		      case '8':
		      case '9':
		       value = (value << 4) + aChar - '0';
		       break;
		      case 'a':
		      case 'b':
		      case 'c':
		      case 'd':
		      case 'e':
		      case 'f':
		       value = (value << 4) + 10 + aChar - 'a';
		       break;
		      case 'A':
		      case 'B':
		      case 'C':
		      case 'D':
		      case 'E':
		      case 'F':
		       value = (value << 4) + 10 + aChar - 'A';
		       break;
		      default:
		       throw new IllegalArgumentException(
		         "Malformed  encoding.");
		      }
		     }
		     outBuffer.append((char) value);
		    } else {
		     if (aChar == 't') {
		      aChar = '\t';
		     } else if (aChar == 'r') {
		      aChar = '\r';
		     } else if (aChar == 'n') {
		      aChar = '\n';
		     } else if (aChar == 'f') {
		      aChar = '\f';
		     }
		     outBuffer.append(aChar);
		    }
		   } else {
		    outBuffer.append(aChar);
		   }
		  }
		  return outBuffer.toString();
	}

	public BlockingQueue<ResourceIP> getIps() {
		return ips;
	}

	public void setIps(BlockingQueue<ResourceIP> ips) {
		this.ips = ips;
	}

	public BlockingQueue<ResourceIP> getConvertIps() {
		return convertIps;
	}

	public void setConvertIps(BlockingQueue<ResourceIP> convertIps) {
		this.convertIps = convertIps;
	}
}

class ReaderResource implements Runnable {
	
	public static final String ORIGIN_SEPERATOR = "\\s+";
	public static final String SEPERATOR = "\t";
	public static final Pattern IP_MATCH_PATTERN = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
	
	private BlockingQueue<ResourceIP> resourceIps = null;
	
	private String filePath ;
	public ReaderResource(String filePath,BlockingQueue<ResourceIP> resourceIps){
		
		this.setFilePath(filePath);
		this.setResourceIps(resourceIps);
		
	}
	
	enum IpCounter {
		TOTAL_RECORD,UN_MATCHE_RECORD,MATCHE_RECORD,FAILED_RECORD,CURRENT_LINE;
	}

	public void run() {
			 File file = new File(filePath);
				if(!file.exists()){
					System.out.println("file not exits");
					return ;
				}
				BufferedReader in = null;
				try{
					in = new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF-8"));
					
					String line = null;
					String[] arr_entry = null;
					ResourceIP resourceIP = null;
					Matcher matcher_ip1 = null;
					Matcher matcher_ip2 = null;
					
					while((line = in.readLine()) != null && !Thread.currentThread().isInterrupted()){
						//parse record
						try{
							arr_entry = line.split(ORIGIN_SEPERATOR);
							matcher_ip1 = IP_MATCH_PATTERN.matcher(arr_entry[0].trim());
							matcher_ip2 = IP_MATCH_PATTERN.matcher(arr_entry[1].trim());
							
							Counters.incrCounter(IpCounter.CURRENT_LINE, 1L);
							
							if(matcher_ip1.matches()&&matcher_ip2.matches()) {
								
								Counters.incrCounter(IpCounter.MATCHE_RECORD, 1L);
								Counters.incrCounter(IpCounter.TOTAL_RECORD, 1L);
								
								resourceIP = new ResourceIP(arr_entry[0].trim(),arr_entry[1].trim());
								resourceIps.put(resourceIP);  //just put the record to the BlockQueue
							} else {
								Counters.incrCounter(IpCounter.UN_MATCHE_RECORD, 1L);
								Counters.incrCounter(IpCounter.TOTAL_RECORD, 1L);
								System.out.println("unmatch record:"+arr_entry[0].trim()+" , "+arr_entry[1].trim());
								continue;
							}
							
						} catch(Exception e) {
							Counters.incrCounter(IpCounter.FAILED_RECORD, 1L);
							Counters.printCounter();
							e.printStackTrace();
							continue;
						}
						
						Thread.sleep(3);
					}
				} catch(IOException e){
					e.printStackTrace();
					return;
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					
					Counters.printCounter();
					
					if(in!=null){ 
						try {
							in.close();
						} catch (IOException e) {
							e.printStackTrace();
							return;
						}
					}
				}
		 }
	
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public BlockingQueue<ResourceIP> getResourceIps() {
		return resourceIps;
	}

	public void setResourceIps(BlockingQueue<ResourceIP> resourceIps) {
		this.resourceIps = resourceIps;
	}
}


class Counters {
	
	private  static ConcurrentHashMap<Enum<?>,Long> counters = new ConcurrentHashMap<Enum<?>, Long>();
	public static void incrCounter(Enum<?> counterGroup,Long incrBy){
		synchronized (counters) {
			if(counters.containsKey(counterGroup)){
				Long preValue = counters.get(counterGroup);
				counters.put(counterGroup, preValue+incrBy);
			} else {
				counters.put(counterGroup, incrBy);
			}
		}
	}
	
	public static void printCounter(){
		System.out.println(counters.toString());
	}
}

class ResourceIP{
	private static final String SEPERATOR = "\t";
	private String beginIP;
	private String endIP;
	private Long beginIpDigital;
	private Long endIpDigital;
	private String jsonData;
	
	public ResourceIP(String beginIP,String endIP) throws Exception{
		this.setBeginIP(beginIP);
		this.setEndIP(endIP);
		this.setBeginIpDigital(convertIp(getBeginIP()));
		this.setEndIpDigital(convertIp(getEndIP()));
	}
	
	 /**
     * ip??????????.
     * 
     * @param ip
     * @return
     */
	private Long convertIp(String ip) throws Exception{
		
	    	long num = 0l;
	        if (ip != null && !"".equals(ip)) {
	            try {
	                String[] moreIp = ip.split(",");
	                if (moreIp.length > 0) {
	                    ip = moreIp[moreIp.length - 1].trim();
	                }
	                String[] ips = ip.split("[.]");
	                num = 16777216L * Long.parseLong(ips[0]) + 65536L * Long.parseLong(ips[1]) + 256
	                        * Long.parseLong(ips[2]) + Long.parseLong(ips[3]);
	            } catch (Exception e) {
	                throw new Exception("ip address error??ip is " + ip);
	            }
	            return num;
	        } else {
	        	throw new Exception("ip is null??");
	        }
	}
	
	public String getEndIP() {
		return endIP;
	}
	public void setEndIP(String endIP) {
		this.endIP = endIP;
	}
	public String getBeginIP() {
		return beginIP;
	}
	public void setBeginIP(String beginIP) {
		this.beginIP = beginIP;
	}

	public Long getBeginIpDigital() {
		return beginIpDigital;
	}

	public void setBeginIpDigital(Long beginIpDigital) {
		this.beginIpDigital = beginIpDigital;
	}

	public Long getEndIpDigital() {
		return endIpDigital;
	}

	public void setEndIpDigital(Long endIpDigital) {
		this.endIpDigital = endIpDigital;
	}

	public String getJsonData() {
		return jsonData;
	}

	public void setJsonData(String jsonData) {
		this.jsonData = jsonData;
	}

	@Override
	public String toString() {
		return beginIpDigital+SEPERATOR+endIpDigital +SEPERATOR+ jsonData;
	}
	
	
}