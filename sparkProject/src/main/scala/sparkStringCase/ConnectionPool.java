package sparkStringCase;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import scalikejdbc.SQL;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Auther icebear
 * @Date 5/30/21
 */

// mysql 连接池
public class ConnectionPool {
    private static ComboPooledDataSource dataSource = new ComboPooledDataSource();
    static{
        // 设置连接数据库的url
        dataSource.setJdbcUrl("jdbc:mysql://node03:3306/testDB");
        // 设置数据库连接名
        dataSource.setUser("root");
        // 设置数据库连接密码
        dataSource.setPassword("123456");
        // 设置连接池最大连接数
        dataSource.setMaxPoolSize(40);
        // 设置连接池最小连接数
        dataSource.setMinPoolSize(2);
        // 设置连接池初始连接数
        dataSource.setInitialPoolSize(10);
        // 设置连接池缓存statement的最大数
        dataSource.setMaxStatements(100);
    }
    // 获取连接
    public static Connection getConnection(){
        try{
            return dataSource.getConnection();
        } catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }
    // 关闭连接
    public static void returnConnection(Connection connection){
        if (connection != null){
            try{
                connection.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
    }
}
