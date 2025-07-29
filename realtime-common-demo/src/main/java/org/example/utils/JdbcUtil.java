package org.example.utils;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.example.constant.Constant;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    // TODO: 获取连接
    public static Connection getJdbcMysqlConnection() throws Exception {
        // 驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 获取连接
        return DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
    }

    // TODO :查询sql 是否进行下换线转 驼峰
    public static <T>List<T> QuerySqlList(Connection connection,String sqlStr ,Class<T> clz,boolean... isUnderlineToCamel ) throws Exception {
        ArrayList<T> objects = new ArrayList<>();
        // 默认不进行转换
        boolean defaultUnderlineToCamel = isUnderlineToCamel.length>0?isUnderlineToCamel[0]:false ;
        PreparedStatement ps = connection.prepareStatement(sqlStr);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            // TODO:通过反射创建一个对象，用于查询结果
            T bean = clz.newInstance();
            for (int i = 1; i < metaData.getColumnCount() ; i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                // 给对象赋值
                if(defaultUnderlineToCamel){
                    columnName= CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(bean,columnName,columnValue);
            }
            objects.add(bean);
        }
        rs.close();
        ps.close();
        return objects;
    }

    // TODO: 关闭连接
    public static void closeJdbcMysqlConnection(Connection connection) throws SQLException {
        if (connection != null ||connection.isClosed() ){
            connection.close();
        }
    }
}
