import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class testJDbc {
    public static void main(String[] args) throws SQLException {

         String jbcUrl = String.format("jdbc:mysql://localhost:3306/ods?&useSSL=false&rewriteBatchedStatements=true");
         Connection connection = DriverManager.getConnection(jbcUrl,"root","123456");
         connection.setAutoCommit(false);

         String insert = "insert into ods.student (id,name,score,age) value (100,'sjl',18,100) ;";
         PreparedStatement statement = connection.prepareStatement(insert);
         statement.execute();
         connection.commit();
         statement.close();
         connection.close();
    }
}
