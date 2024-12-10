const express = require("express");
const sql = require("mssql");
const cors = require("cors");

const app = express();
const port = 5000;

// Cấu hình kết nối SQL Server
const dbConfig = {
  user: "sa",
  password: "sa",
  server: "localhost",
  database: "datamart_db",
  options: {
    encrypt: true, // nếu sử dụng Azure
    trustServerCertificate: true, // cho server cục bộ
  },
};

// Middleware
app.use(cors());
app.use(express.json());

// Endpoint lấy danh sách sản phẩm
app.get("/api/products", async (req, res) => {
  try {
    const pool = await sql.connect(dbConfig);
    console.log(pool);
    const result = await pool
      .request()
      .query(
        "SELECT * FROM product_dim as pro join specification_dim as spe on pro.id = spe.id_product"
      ); // Bảng "Products"
    res.json(result.recordset);
  } catch (error) {
    res.status(500).send("Error retrieving products: " + error.message);
  }
});

// Chạy server
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
