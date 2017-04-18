package cn.celloud.j2ee.test.day01;

import org.apache.http.HttpEntity;  
import org.apache.http.HttpResponse;  
import org.apache.http.client.methods.HttpGet;  
import org.apache.http.impl.client.DefaultHttpClient;  
  
import java.io.*;  

public class DownLoad {  
    private String workPath = null;  
    private String downLoadUrl = null;  
    private DefaultHttpClient httpClient = new DefaultHttpClient();  
  
    public void downLoad(String url, String dst) {  
        try {
            HttpGet httpGet = new HttpGet(url);  
            HttpResponse httpResponse = httpClient.execute(httpGet);  
            HttpEntity entity = httpResponse.getEntity();
            InputStream in = entity.getContent();  
            long length=entity.getContentLength();  
            if(length<=0){  
                System.out.println("下载文件不存在！");  
                return;  
            }  
            OutputStream out = new FileOutputStream(new File(dst));  
            saveTo(in, out);  
        } catch (IOException e) {  
            e.printStackTrace();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void saveTo(InputStream in, OutputStream out) throws Exception {  
        byte[] data = new byte[1024*1024];  
        int index =0;  
        while ((index=in.read(data) )!= -1) {  
            out.write(data,0,index);  
        }  
        in.close();  
        out.close();  
    }  
  
    public static void main(String args[]) {  
        DownLoad downLoad = new DownLoad();  
//        String url1 = "http://localhost:8080/celloud/api/report/pdf?serialNumber=celloud-1&dataKey=17021505867466";
        String url2 = "http://www.gjt.org/download/time/java/tar/javatar-2.5.tar.gz";  
//        downLoad.downLoad(url1, "E:\\xxx.pdf");
        downLoad.downLoad(url2, "E:\\javatar-2.5.tar.gz");  
    }  
}  
