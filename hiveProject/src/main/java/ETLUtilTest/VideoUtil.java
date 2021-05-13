/**
 * @Auther icebear
 * @Date 5/13/21
 */

package ETLUtilTest;

public class VideoUtil {
    /**
     * 数据切割，如果长度小于9 直接丢弃
     * 视频类别中间的空格去掉
     * 关联视频，使用 & 进行分割
     * FM1KUDE3C3k  renetto	736	News & Politics	1063	9062	4.57	525	488	LnMvSxl0o0A&IKMtzNuKQso&Bq8ubu7WHkY&Su0VTfwia1w&0SNRfquDfZs&C72NVoPsRGw
     */
    // 数据清洗方法
    public static String washDatas(String line){
        // 判断读入的数据是否为空
        if(null == line || "".equals(line)){
            return null;
        }
        // 判断数据长度，如果小于9，丢弃
        String[] split = line.split("\t");
        if(split.length < 9){
            return null;
        }
        // 将视频类别空格去掉
        split[3] = split[3].replace(" ", "");
        StringBuilder builder = new StringBuilder();
        // 数据字段的提取
        for(int i=0; i<split.length; i++){
            if(i < 9){
                // 获取前八个字段
                builder.append(split[i]).append("\t");
            }else if(i >= 9 && i < split.length - 1){
                builder.append(split[i]).append("&");
            }else if(i == split.length - 1){
                builder.append(split[i]);
            }
        }
        return builder.toString();
    }
}
