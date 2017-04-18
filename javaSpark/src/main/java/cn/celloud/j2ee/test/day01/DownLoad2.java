package cn.celloud.j2ee.test.day01;

import org.apache.http.HttpResponse;  
import org.apache.http.client.methods.HttpGet;  
import org.apache.http.impl.client.DefaultHttpClient;  
  
import java.io.*;  

public class DownLoad2 {  
    private DefaultHttpClient httpClient = new DefaultHttpClient();  
  
    public void downLoad(String url) {  
        try {  
            HttpGet httpGet = new HttpGet(url);  
            HttpResponse httpResponse = httpClient.execute(httpGet);  
            if(httpResponse.getEntity().isStreaming()){
            	String target = "E:\\javatar-2.5.tar.gz";
            	FileOutputStream out = new FileOutputStream(target);
            	httpResponse.getEntity().writeTo(out);
            }
        } catch (IOException e) {  
            e.printStackTrace();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
  
  
    public static void main(String args[]) {  
        DownLoad2 d = new DownLoad2();  
        String url2 = "http://www.gjt.org/download/time/java/tar/javatar-2.5.tar.gz";  
        d.downLoad(url2);  
    }  
}  
