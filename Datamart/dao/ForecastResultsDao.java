package dao;

import entity.Config;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ForecastResultsDao {

    public static List<Config> getConfigs(Connection connection) {
        List<Config> configs = new ArrayList<>();
        //Câu select lấy list config muốn run
        String query = "SELECT * FROM config WHERE flag = 1 ORDER BY update_at DESC";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name_config = resultSet.getString("name_config");
                String descriptionail = resultSet.getString("description");
                String source_path = resultSet.getString("source_path");
                String format = resultSet.getString("format");
                String file_name = resultSet.getString("file_name");
                String directory_file = resultSet.getString("directory_file");
                String create_by = resultSet.getString("create_by");
                Timestamp timestamp = resultSet.getTimestamp("update_at");
                configs.add(new Config(id, name_config, description, source_path, format, file_name, directory_file, create_by, timestamp));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return configs;
    }

    public static void updateStatusProcess(Connection connection, int id, String statusprocess) {
        try (CallableStatement callableStatement = connection.prepareCall("{CALL Updatestatus_process(?,?)}")) {
            callableStatement.setInt(1, id);
            callableStatement.setString(2, statusprocess);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateDescription(Connection connection, int id, String description) {
        try (CallableStatement callableStatement = connection.prepareCall("{CALL UpdatePathFileDetail(?,?)}")) {
            callableStatement.setInt(1, id);
            callableStatement.setString(2, description);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void insertFileLogs(Connection connection, int idFileConfig, String statusprocess) {
        try (CallableStatement callableStatement = connection.prepareCall("{Call InsertLog(?,?,?)}")) {
            callableStatement.setInt(1, idFileConfig);
            callableStatement.setString(2, statusprocess);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getLogs(Connection connection, int idFileConfig) {
        List<String> logs = new ArrayList<>();
        //Câu select lấy list config muốn run
        String query = "SELECT * FROM log WHERE id_file_config = ? ORDER BY created_at ASC";
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(1, idFileConfig);
            ResultSet resultSet = statement.executeQuery();
            int i = 1;
            while (resultSet.next()) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(i++ + ". ");
                stringBuilder.append("ID FileConfig: " + resultSet.getInt("id_file_config"));
                stringBuilder.append("statusprocess: " + resultSet.getString("status_process"));
                stringBuilder.append("Time: " + resultSet.getTimestamp("created_at").toString());
                logs.add(stringBuilder.toString());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return logs;
    }
}
