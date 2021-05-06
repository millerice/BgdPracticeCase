/**
 * @Auther icebear
 * @Date 5/6/21
 */

package ice.bear;

import java.io.IOException;
import java.sql.*;

public class PhoenixSearch {
    // 定义变量
    // java sql的 connecton
    private Connection connection;
    private Statement statement;
    private ResultSet rs;
    // 定义init()初始化方法
    public void init() throws SQLException{
        // 定义phoenix的连接地址
        String url = "jdbc:phoenix:node01:2181";
        connection = DriverManager.getConnection(url);
        // 构建Statement对象
        statement = connection.createStatement();
    }
    // 构建查询方法
    public void queryTable() throws SQLException{
        // 定义查询语句，注意大小写
        String sql = "select * from 'employee'";
        // 执行sql语句
        try{
            rs = statement.executeQuery(sql);
            while (rs.next()){
                System.out.println("name:" + rs.getString("name"));
                System.out.println("position:" + rs.getString("position"));
                System.out.println("tel:" + rs.getString("tel"));
                System.out.println("------------------------------");
            }
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            if (connection!=null){
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws SQLException, NullPointerException {
        PhoenixSearch phoenixSearch = new PhoenixSearch();
        phoenixSearch.queryTable();
    }
}

