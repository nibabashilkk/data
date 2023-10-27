package util;

import java.sql.Connection;
import java.sql.SQLException;
import com.alibaba.druid.pool.DruidDataSource;
import entity.Convert;

public class DBUtil {
    // 声明druid连接池对象
    private DruidDataSource pool;

    /** 数据库 链接URL地址 **/
    private String url;
    /** 账号 **/
    private String username;
    /** 密码 **/
    private String password;
    /** 初始连接数 **/
    private int initialSize = 16;
    /** 最大活动连接数 **/
    private int maxActive = 16;
    /** 最小闲置连接数 **/
    private int minIdle = 16;
    /** 连接耗尽时最大等待获取连接时间 **/
    private long maxWait = 60000;

    private Convert convert;

    public DBUtil(Convert convert){
        this.convert = convert;
        init(convert);
    }

    private void loadProp(Convert convert) {
        url = convert.getJdbc();
        username = convert.getSourceUser();
        password = convert.getSourcePasswd();
    }

    private void init(Convert convert) {
        pool = new DruidDataSource();
        // 加载属性文件,初始化配置
        loadProp(convert);
        pool.setUrl(url);
        pool.setUsername(username);
        pool.setPassword(password);

        // 设置连接池中初始连接数
        pool.setInitialSize(initialSize);
        // 设置最大连接数
        pool.setMaxActive(maxActive);
        // 设置最小的闲置链接数
        pool.setMinIdle(minIdle);
        // 设置最大的等待时间(等待获取链接的时间)
        pool.setMaxWait(maxWait);
        pool.setDriverClassName("com.informix.jdbc.IfxDriver");
        pool.setValidationQuery("select count(*) from systables");
    }

    public Connection getConn() {
        try {
            if (pool == null || pool.isClosed()) {
                init(convert);
            }
            return pool.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;

    }

    public void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
