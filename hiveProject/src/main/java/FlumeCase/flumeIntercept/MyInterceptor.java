/**
 * @Auther icebear
 * @Date 5/17/21
 */

package FlumeCase.flumeIntercept;

import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class MyInterceptor implements Interceptor {
    // 指定需要加密的字段下标
    private final String encrypted_field_index;
    // 指定不需要对应列的下标
    private final String out_index;
    // 提供构建方法，后期接收配置文件中的参数
    public MyInterceptor(String encrypted_field_index, String out_index){
        this.encrypted_field_index=encrypted_field_index.trim();
        this.out_index=out_index.trim();
    }
    // 单个event拦截逻辑
    public Event intercept(Event event){
        if (event == null){
            return null;
        }
        try{
            String line = new String(event.getBody(), Charsets.UTF_8);
            String[] fields = line.split(",");

            String newLine = "";
            for (int i = 0; i < fields.length; i++){
                // 字符串数字转换成int
                int encryptedField = Integer.parseInt(encrypted_field_index);
                int outIndex = Integer.parseInt(out_index);
                // 对数据进行处理（脱敏、清洗）
                if (i == encryptedField){
                    newLine += md5(fields[i]) + ",";
                }else if (i != outIndex){
                    newLine += fields[i] + ",";
                }
            }
            // 处理后的字段
            newLine = newLine.substring(0, newLine.length()-1);
            // 设置utf8编码
            event.setBody(newLine.getBytes(Charsets.UTF_8));
            // 返回event
            return event;
        }catch (Exception e){
            return event;
        }
    }
    // 批量event拦截逻辑
    public List<Event> intercept(List<Event> events){
        List<Event> out = new ArrayList<Event>();
        for (Event event:events){
            Event outEvent = intercept(event);
            if (outEvent != null){
                out.add(outEvent);
            }
        }
        return out;
    }
    public void close(){

    }
    public void initialize(){

    }
    // md5加密方法
    public static String md5(String plainText){
        // 定义一个字节数组
        byte[] secretBytes = null;
        try{
            // 生成一个MD5加密算法摘要
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 对字符串进行加密
            md.update(plainText.getBytes());
            // 获得加密后的数据
            secretBytes = md.digest();
        }catch (NoSuchAlgorithmException e){
            throw new RuntimeException("没有md5算法～");
        }
        // 将加密后的数据转换为16进制数字
        String md5code = new BigInteger(1, secretBytes).toString(16);
        // 如果生成数字未满32位，需要前面补0
        for (int i = 0; i < 32 - md5code.length(); i++){
            md5code = "0" + md5code;
        }
        return md5code;
    }
    // 在flume采集配置文件中通过制定该Builder来创建Interceptor对象
    public static class MyBuilder implements Interceptor.Builder{
        // 指定需要加密的字段下标
        private String encrypted_field_index;
        // 指定不需要的字段下标
        private String out_index;
        // 从数据中获取对应坐标
        public void configure(Context context){
            this.encrypted_field_index = context.getString("encrypted_field_index", "");
            this.out_index = context.getString("out_index", "");
        }
        // 使用拦截器对数据进行过滤
        public MyInterceptor build(){
            return new MyInterceptor(encrypted_field_index, out_index);
        }
    }

}
