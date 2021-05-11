import java.sql.*;

/**
 * @Auther icebear
 * @Date 5/11/21
 */

public class HiveJDBC {
    private static String url="jdbc:hive2://172.16.151.4:10000/myhive";
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        //获取数据库连接
        Connection connection = DriverManager.getConnection(url, "root","123456");
        //定义查询的sql语句
        String sql="select * from stu";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                //获取id字段值
                int id = rs.getInt(1);
                //获取deptid字段
                String name = rs.getString(2);
                System.out.println(id+"\t"+name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
