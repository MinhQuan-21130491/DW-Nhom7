package LoadToStaging;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Script {
	// URL cho SQL Server
	private static final String DB_CONTROL_URL = "jdbc:sqlserver://localhost:1433;databaseName=control_db;encrypt=true;trustServerCertificate=true";
	private static final String DB_STAGING_URL = "jdbc:sqlserver://localhost:1433;databaseName=staging_db;encrypt=true;trustServerCertificate=true";

	// Thông tin đăng nhập
	private static final String DB_USER = "sa";
	private static final String DB_PASSWORD = "sa";

	public static void main(String[] args) {
		Connection dbControlConnection = null;
		Connection dbStagingConnection = null;

		try {
			// 1. Kết nối control database
			try {
                dbControlConnection = DriverManager.getConnection(DB_CONTROL_URL, DB_USER, DB_PASSWORD);
                System.out.println("Connected to control_db successfully.");
			// 2.kiểm tra kết nối
                


            } catch (SQLException e) {
                sendEmail("Connect to database control failed");
//                System.err.println("SQL Error: " + e.getMessage());
//                e.printStackTrace(); // In ra stack trace chi tiết của lỗi
                return;
            }
            
			// 3. Kiểm tra crawl data trong ngày
			if (!isCrawlSuccessful(dbControlConnection)) {
				logToLogTable(dbControlConnection, "Load to staging failed", "Data has not been crawled today",
						LocalDateTime.now(), "Ai Nhung");
				sendEmail("Connect to database control is failed");
				return;
			}
			// 4. Kiểm tra trạng thái `LoadToStaging` trong ngày
            if (isLoadToStagingRunning(dbControlConnection)) {
//                logToLogTable(dbControlConnection, 0, "Skipped", "LoadToStaging already ran today", LocalDateTime.now(), null);
                sendEmail("LoadToStaging already ran today. Program exited.");
                return; // Thoát chương trình nếu đã chạy
            } else {
//            	5. Viết log
                logToLogTable(dbControlConnection, "Loading data to staging", "LoadToStaging started", LocalDateTime.now(), null);
            }
         // 6. Lấy thông tin file đã được crawl thành công
            FileConfig fileConfig = getFileConfig(dbControlConnection);
            if (fileConfig == null) {
                logToLogTable(dbControlConnection, "Failed", "Failed to retrieve file information", LocalDateTime.now(), null);
                sendEmail("4 Failed to retrieve file information.");
                return;
            }

            // Kiểm tra sự tồn tại của FileConfig ID trong bảng file_configs
//            if (!isFileConfigExists(dbControlConnection, fileConfig.id)) {
//                logToLogTable(dbControlConnection, fileConfig.id, "Failed", "FileConfig ID does not exist in file_configs table", LocalDateTime.now(), null);
//                sendEmail("5 FileConfig ID does not exist in file_configs table.");
//                return;
//            }

            //7. Lấy đường dẫn hoàn chỉnh
            String fullFilePath = getFullFilePath(fileConfig);
            System.out.println("File path: " + fullFilePath);

			// 8. Kết nối staging database
			try {
				dbStagingConnection = DriverManager.getConnection(DB_STAGING_URL, DB_USER, DB_PASSWORD);
				System.out.println("Connected to staging_db successfully.");
			} catch (SQLException e) {
//                logToLogTable(dbControlConnection, fileConfig.id, "Failed", "Failed to connect to staging_db", LocalDateTime.now(), null);
				sendEmail("Failed to connect to DB Staging");
				return;
			}

			// 5. Kiểm tra và xóa dữ liệu cũ trong staging (nếu có)
			if (isStagingDataExists(dbStagingConnection)) {
				deleteStagingData(dbStagingConnection);
			}

			// 6. Ghi dữ liệu mới vào staging
			if (loadDataToStaging(dbStagingConnection, fileConfig)) {
				logToLogTable(dbControlConnection, "Load to staging successfully", "Data loaded successfully",
						LocalDateTime.now(), "Ai Nhung");
				sendEmail("6 Data loaded successfully into staging");
			} else {
				logToLogTable(dbControlConnection, "Load data to staging failed", "Data load failed", LocalDateTime.now(),
						"nhung");
				sendEmail("7 Data load failed into staging_laptop");
			}

		} catch (SQLException e) {
			System.err.println("Error: " + e.getMessage());
		} finally {
			// 7. Đóng kết nối
			closeConnections(dbControlConnection, dbStagingConnection);
		}
	}

	// Hàm kiểm tra trạng thái crawl logs
	private static boolean isCrawlSuccessful(Connection connection) {
		String query = "SELECT COUNT(*) FROM file_logs WHERE CONVERT(date, create_at) = CONVERT(date, GETDATE()) AND status_process = 'Save data successfully'";
		try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
			return resultSet.next() && resultSet.getInt(1) > 0;
		} catch (SQLException e) {
			System.err.println("Error checking crawl logs: " + e.getMessage());
		}
		return false;
	}

	// Hàm kiểm tra trạng thái `LoadToStaging`
	private static boolean isLoadToStagingRunning(Connection connection) {
		String query = "SELECT COUNT(*) FROM file_logs WHERE CONVERT(date, create_at) = CONVERT(date, GETDATE()) AND status_process = 'Loading data to staging'";
		try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
			return resultSet.next() && resultSet.getInt(1) > 0;
		} catch (SQLException e) {
			System.err.println("Error checking LoadToStaging status: " + e.getMessage());
		}
		return false;
	}

	// Hàm lấy thông tin file đã crawl thành công trong ngày
	private static FileConfig getFileConfig(Connection connection) throws SQLException {
	    String query = "SELECT id, directory_file, file_name FROM file_configs WHERE id IN (SELECT id_file_config FROM file_logs WHERE CONVERT(date, create_at) = CONVERT(date, GETDATE()) AND status_process = 'Save data successfully')";
	    try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
	        if (resultSet.next()) {
	            int id = resultSet.getInt("id");
	            String directoryFile = resultSet.getString("directory_file");
	            String filename = resultSet.getString("file_name");
	            return new FileConfig( directoryFile, filename);
	        }
	    }
	    return null;
	}
	// Hàm lấy đường dẫn đầy đủ của file
	private static String getFullFilePath(FileConfig fileConfig) {
	    // Định dạng ngày tháng theo mẫu dd_MM_yyyy
	    String fileDate = LocalDate.now().format(DateTimeFormatter.ofPattern("dd_MM_yyyy"));
	    return String.format("%s\\%s_%s.csv", fileConfig.directoryFile, fileConfig.filename, fileDate);
	}


	// Hàm kiểm tra dữ liệu cũ trong staging
	private static boolean isStagingDataExists(Connection connection) throws SQLException {
		String query = "SELECT COUNT(*) FROM aggregate_products";
		try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
			return resultSet.next() && resultSet.getInt(1) > 0;
		}
	}

	// Hàm xóa dữ liệu cũ trong staging
	private static void deleteStagingData(Connection connection) throws SQLException {
		String query = "DELETE FROM aggregate_products";
		try (Statement statement = connection.createStatement()) {
			statement.executeUpdate(query);
			System.out.println("Old data deleted from staging table.");
		}
	}
	//Hàm đọc và ghi dữ liệu vào staging
	private static boolean loadDataToStaging(Connection connection, FileConfig config) {
	    String filePath = getFullFilePath(config);  // Lấy đường dẫn đầy đủ
	    String query = String.format(
	            "BULK INSERT aggregate_products FROM '%s' WITH (FIELDTERMINATOR = ';', ROWTERMINATOR = '\\n', FIRSTROW = 2, TEXTQUALIFIER = '\"')",
	            filePath.replace("\\", "\\\\")); 
	    try (Statement statement = connection.createStatement()) {
	        statement.execute(query);
	        System.out.println("Data loaded into staging table successfully.");
	        return true;
	    } catch (SQLException e) {
	        System.err.println("Failed to load data into staging: " + e.getMessage());
	    }
	    return false;
	}



	// Hàm ghi log
	private static void logToLogTable(Connection connection, String status, String note,
			LocalDateTime createdAt, String createdBy) {
		// Kiểm tra sự tồn tại của FileConfig trước khi ghi log
//		if (!isFileConfigExists(connection, configId)) {
//			System.err.println("FileConfig ID " + configId + " does not exist in file_configs table.");
//			return; // Dừng lại nếu không tìm thấy FileConfig hợp lệ
//		}
		
		// Nếu createdBy là null, thay thế bằng giá trị mặc định
		if (createdBy == null) {
			createdBy = "System";
		}
		
		String query = "INSERT INTO file_logs (status_process, note, create_at, created_by) VALUES ( ?, ?, ?, ?)";
		try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
//			preparedStatement.setInt(1, configId);
			preparedStatement.setString(1, status);
			preparedStatement.setString(2, note);
			preparedStatement.setTimestamp(3, Timestamp.valueOf(createdAt));
			preparedStatement.setString(4, "Ai Nhung");
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			System.err.println("Error logging to table: " + e.getMessage());
		}
	}

	// Hàm gửi email (mock)
	private static void sendEmail(String message) {
		System.out.println("Email sent: " + message);
	}

	// Hàm đóng kết nối
	private static void closeConnections(Connection... connections) {
		for (Connection connection : connections) {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					System.err.println("Error closing connection: " + e.getMessage());
				}
			}
		}
	}

	// Lớp lưu thông tin file config
	private static class FileConfig {
		int id;
		String directoryFile;
		String filename;

		FileConfig(String directoryFile, String filename) {
			this.id = id;
			this.directoryFile = directoryFile;
			this.filename = filename;
		}
	}
	
	// Hàm kiểm tra sự tồn tại của FileConfig
	// Mới thêm vào để kiểm tra nếu FileConfig tồn tại trong bảng file_configs
	private static boolean isFileConfigExists(Connection connection, int configId) {
	    String query = "SELECT COUNT(*) FROM file_configs WHERE id = ?";
	    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
	        preparedStatement.setInt(1, configId);
	        try (ResultSet resultSet = preparedStatement.executeQuery()) {
	            return resultSet.next() && resultSet.getInt(1) > 0;
	        }
	    } catch (SQLException e) {
	        System.err.println("Error checking if FileConfig exists: " + e.getMessage());
	    }
	    return false;
	}
}
