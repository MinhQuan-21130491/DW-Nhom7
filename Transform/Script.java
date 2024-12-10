package Transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Script {

    private static final Logger LOGGER = Logger.getLogger(Transform.class.getName());

    public static void main(String[] args) {
        String configPath = "config.json"; // Đường dẫn file cấu hình

        try {
            // Bước 1: Đọc file cấu hình
            Config config = readConfig(configPath);

            // Bước 2: Kiểm tra flag
            if (!config.flag.equals("ready")) {
                logMessage("Flag is not ready. Exiting...");
                return;
            }

            updateFlag(configPath, "activating");

            // Bước 3: Kết nối cơ sở dữ liệu
            Connection stagingConn = connectToDb(config.stagingDbUrl);
            Connection warehouseConn = connectToDb(config.warehouseDbUrl);

            if (stagingConn == null || warehouseConn == null) {
                logMessage("Failed to connect to databases.");
                updateFlag(configPath, "failed");
                return;
            }

            // Bước 4: Thực hiện ETL
            runETLProcess(stagingConn, warehouseConn, configPath);

        } catch (Exception e) {
            logMessage("ETL process failed: " + e.getMessage());
        }
    }

    // Hàm đọc file cấu hình
    public static Config readConfig(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(filePath), Config.class);
    }

    // Hàm cập nhật flag trong file cấu hình
    public static void updateFlag(String filePath, String flagValue) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(filePath));
        ((ObjectNode) root).put("flag", flagValue);
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(filePath), root);
    }

    // Hàm ghi log
    public static void logMessage(String message) {
        LOGGER.log(Level.INFO, message);
    }

    // Hàm kết nối cơ sở dữ liệu
    public static Connection connectToDb(String dbUrl) {
        try {
            return DriverManager.getConnection(dbUrl);
        } catch (SQLException e) {
            logMessage("Error connecting to database: " + e.getMessage());
            return null;
        }
    }

    // Hàm chạy ETL
    public static void runETLProcess(Connection stagingConn, Connection warehouseConn, String configPath) {
        try {
            // Bước 1: Đọc dữ liệu từ Staging DB
            String query = "SELECT * FROM product";
            Statement stmt = stagingConn.createStatement();
            ResultSet resultSet = stmt.executeQuery(query);

            List<Product> products = new ArrayList<>();
            while (resultSet.next()) {
                Product product = new Product(
                        resultSet.getInt("id"),
                        resultSet.getString("name"),
                        resultSet.getDouble("price")
                );
                products.add(product);
            }

            // Bước 2: Xử lý dữ liệu
            List<Product> transformedData = transformData(products);

            // Bước 3: Tải dữ liệu vào Warehouse DB
            saveToWarehouse(transformedData, warehouseConn);

            // Cập nhật flag thành công
            updateFlag(configPath, "succeed");
            logMessage("ETL process completed successfully.");

        } catch (Exception e) {
            logMessage("ETL process failed: " + e.getMessage());
            try {
                updateFlag(configPath, "failed");
            } catch (IOException ex) {
                logMessage("Failed to update flag: " + ex.getMessage());
            }
        }
    }

    // Hàm xử lý dữ liệu
    public static List<Product> transformData(List<Product> products) {
        List<Product> transformedData = new ArrayList<>();
        for (Product product : products) {
            if (product.getId() != null && product.getName() != null) {
                product.setName(product.getName().trim());
                if (product.getPrice() == null) {
                    product.setPrice(0.0);
                }
                transformedData.add(product);
            }
        }
        return transformedData;
    }

    // Hàm lưu dữ liệu vào Warehouse
    public static void saveToWarehouse(List<Product> products, Connection warehouseConn) throws SQLException {
        String insertQuery = "INSERT INTO product_dim (id, name, price) VALUES (?, ?, ?)";
        PreparedStatement pstmt = warehouseConn.prepareStatement(insertQuery);

        for (Product product : products) {
            pstmt.setInt(1, product.getId());
            pstmt.setString(2, product.getName());
            pstmt.setDouble(3, product.getPrice());
            pstmt.executeUpdate();
        }
    }
}
