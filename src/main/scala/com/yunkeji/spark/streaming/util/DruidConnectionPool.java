package com.yunkeji.spark.streaming.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidConnectionPool {

    private transient static DataSource dataSource = null;

    private transient static Properties props = new Properties();

    static {
        props.put("driverClassName", "com.mysql.jdbc.Driver");
        props.put("url", "jdbc:mysql://hadoop101:3306/test?characterEncoding=UTF-8");
        props.put("username", "root");
        props.put("password", "root");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private DruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}