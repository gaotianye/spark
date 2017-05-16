/*package cn.celloud.j2ee.test.day01;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.sms.model.v20160927.SingleSendSmsRequest;
import com.aliyuncs.sms.model.v20160927.SingleSendSmsResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;

public class AliyunMessage {
	public static void main(String[] args) {
		test();
	}
	public static void test() {
		try {
			IClientProfile profile = DefaultProfile.getProfile("cn-hangzhou", "UrKY3uHmTuwajVKU", "zK4cU4cEHPEnQNxu5NnnmCSvBHXuOd");
			DefaultProfile.addEndpoint("cn-hangzhou", "cn-hangzhou", "Sms", "sms.aliyuncs.com");
			IAcsClient client = new DefaultAcsClient(profile);
			SingleSendSmsRequest request = new SingleSendSmsRequest();
			request.setSignName("华点云");// 控制台创建的签名名称
			request.setTemplateCode("SMS_22495201");// 控制台创建的模板CODE
			request.setParamString("{\"customer\":\"高天野\"}");// 短信模板中的变量；数字需要转换为字符串；个人用户每个变量长度必须小于15个字符。"
			request.setRecNum("15156506755");// 接收号码
			SingleSendSmsResponse httpResponse = client.getAcsResponse(request);
			System.out.println(httpResponse.toString());
		} catch (ServerException e) {
			e.printStackTrace();
		} catch (ClientException e) {
			e.printStackTrace();
		}
	}
}
*/