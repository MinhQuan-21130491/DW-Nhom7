package controller;

import dao.ForecastResultsDao;
import database.DBConnection;
import entity.Config;
import util.SendMail;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static util.CreateFileLog.createFIleLog;

public class Controller {
    // Configuration file path
    private static final String FILE_CONFIG = "\\config.env";

    // API Key, URL, and list of cities
    static String apiKey;
    static String url;
    static List<String> cities;
    // Load attributes from the configuration file
    public static void loadAttribute(){
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            String currentDir = System.getProperty("user.dir");
            inputStream = new FileInputStream(currentDir + FILE_CONFIG);
            // load properties from file
            properties.load(inputStream);
            // get property by name
            apiKey = properties.getProperty("apiKey"); //key của account trên openweather
            url = properties.getProperty("url"); //url lấy dữ liệu
            cities = convertCities(properties.getProperty("cities")); //danh sách các khu vực muốn lấy dữ liệu
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // close objects
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static List<String> convertCities(String cities){
        // Split the string into an array of strings, trim each string, and then collect into a list
        return Arrays.stream(cities.split(",")).map(String::trim).collect(Collectors.toList());
    }

    public static void loadToDataMart(Connection connection, Config config){
        ForecastResultsDao dao = new ForecastResultsDao();
        //(Load To DataMart) 10. Cập nhật trạng thái của config là đang xử lý(Pre-processing=true)
        dao.updateIsProcessing(connection, config.getId(), true);
        //(Load To DataMart)11. Cập nhật status của config thành MLOADING (status=MLOADING)
        dao.updateStatus(connection, config.getId(), "MLOADING");
        //(Load To DataMart)12. Thêm thông tin bắt đầu load to datamart vào log
        dao.insertLog(connection, config.getId(), "MLOADING", "Start load data to DataMart");
        //(Load To DataMart)13. Load Data To DataMart
        try(CallableStatement callableStatement = connection.prepareCall("{CALL LoadToDM()}")){
            callableStatement.execute();
            //(Load To DataMart)14. Cập nhật status của config thành MLOADED
            dao.updateStatus(connection, config.getId(), "MLOADED");
            //(Load To DataMart)15. Thêm thông tin đã load data to datamart vào log
            dao.insertLog(connection, config.getId(), "MLOADED", "Load to mart success");
            System.out.println("load to mart success!");
            //finish
            //(Load To DataMart)16. Cập nhật status của config thành FINISHED
            dao.updateStatus(connection, config.getId(), "FINISHED");
            //(Load To DataMart)17. Thêm thông tin đã hoàn thành tiến trình vào log
            dao.insertLog(connection, config.getId(), "FINISHED", "Finished!");
            //(Load To DataMart)18. Cập nhật trạng thái của config là không xử lý(Pre-processing=false)
            dao.updateIsProcessing(connection, config.getId(), false);
            //(Load To DataMart)19. Send mail thông báo tiến trình hoàn tất cho email của create_by
            //send mail khi đã hoàn thành việc lấy data load vào warehouse
            String mail = config.getEmail();
            DateTimeFormatter dt = DateTimeFormatter.ofPattern("hh:mm:ss dd/MM/yyyy");
            LocalDateTime nowTime = LocalDateTime.now();
            String timeNow = nowTime.format(dt);
            String subject = "Success DataWarehouse Date: " + timeNow;
            String message = "Success";
            String pathLogs = createFIleLog(dao.getLogs(connection, config.getId()));
            if(pathLogs!=null){
                SendMail.sendMail(mail, subject, message, pathLogs);
            }
            else SendMail.sendMail(mail, subject, message);
        }catch (SQLException e){
            e.printStackTrace();
            //(Load To DataMart)20. Cập nhật status của config thành ERROR
            dao.updateStatus(connection, config.getId(), "ERROR");
            //(Load To DataMart)21. Thêm lỗi vào log
            dao.insertLog(connection, config.getId(), "ERROR", "Error with message: "+e.getMessage());
            //(Load To DataMart) 22. Cập nhật trạng thái của config là không xử lý(Pre-processing=false)
            dao.updateIsProcessing(connection, config.getId(), false);
            //(Load To DataMart) 23. Send mail thông báo lỗi cho email của create_by
            //send mail
            String mail = config.getEmail();
            DateTimeFormatter dt = DateTimeFormatter.ofPattern("hh:mm:ss dd/MM/yyyy");
            LocalDateTime nowTime = LocalDateTime.now();
            String timeNow = nowTime.format(dt);
            String subject = "Error Date: " + timeNow;
            String message = "Error with message: "+e.getMessage();
            String pathLogs = createFIleLog(dao.getLogs(connection, config.getId()));
            if(pathLogs!=null){
                SendMail.sendMail(mail, subject, message, pathLogs);
            }
            else SendMail.sendMail(mail, subject, message);
        }
    }

}
