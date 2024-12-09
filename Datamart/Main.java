import controller.Controller;
import dao.ForecastResultsDao;
import database.DBConnection;
import entity.Config;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        DBConnection db = new DBConnection();
        ForecastResultsDao dao = new ForecastResultsDao();
        //2. Kết nối với với database Controller
        try (Connection connection = db.getConnection()) {
            //3. Duyệt for lấy lần lượt từng config trong list
            for (Config config : configs) {
                int maxWait = 0;
                //5. Khi có processing nào chạy và thời gian dưới 3 phút
                while (dao.getProcessingCount(connection) != 0 && maxWait <= 3) {
                    System.out.println("Wait...");
                    // 6. Chờ 1 phút, tăng biến thời gian
                    maxWait++;
                    Thread.sleep(60000); //60s
                }
                //7. Kiểm tra xem còn processing nào đang chạy không
                if (dao.getProcessingCount(connection) == 0) { //Hết process đang chạy
                    System.out.println("Start");
                    //8. Lấy status của config
                    String status = config.getStatus();
                    //Nếu lỗi thì không cần thực hiện
                    if (status.equals("ERROR")) {
                        continue;
                    }
                    //(Load To DataMart)9. Kiểm tra xem status có phải là AGGREGATED hay không
                    else if (status.equals("AGGREGATED")) {
                        controller.loadToDataMart(connection, config);
                    }
                }
            }
            // 4. Đóng kết nối database
            db.closeConnection();
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
