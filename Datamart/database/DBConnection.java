package database;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnection {
    private static final String FILE_CONFIG = "\\config.env";
    private static String urlDb;
    private static String db;
    private static String host;
    private static String port;
    private static String nameDB;
    private static String username;
    private static String password;
    private static Connection connection;
    static {
        //   1. Load config module trong file .env
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            String currentDir = System.getProperty("user.dir");
            inputStream = new FileInputStream(currentDir + FILE_CONFIG);
            // load properties from file
            properties.load(inputStream);
            // get property by name
            db = properties.getProperty("db"); //tên hệ quản trị cơ sở dữ liệu
            host = properties.getProperty("host"); // host dể kết nối CSDL
            port = properties.getProperty("port"); // port để kết nối với CSDL
            nameDB = properties.getProperty("name_database"); // tên database muốn kết nối

            urlDb = "jdbc:"+db+"://"+host+":"+port+"/"+nameDB;

            username = properties.getProperty("username"); // tên đăng nhập để kết nối với db
            password = properties.getProperty("password"); // password để dăng nhập
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // close objects - khi lấy xong các giá trị thuộc tính thì đóng kết nối input stream
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public DBConnection(){

    }

    //Kết nối với database
    public static Connection getConnection(){
        if (connection == null) {
            try {
                // connect
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(urlDb, username, password);
            } catch (SQLException e) {
                throw new RuntimeException(e.getMessage());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return connection;
    }

    //Đóng kết nối
    public static void closeConnection(){
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Close Connection");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
