const axios = require("axios");
const cheerio = require("cheerio");
const { log } = require("console");
const fs = require("fs");
const { Parser } = require("json2csv");
require("dotenv").config({ path: "D:/DataWareHouse/crawl-data/.env" });
const sql = require("mssql");
const nodemailer = require("nodemailer");

//1. Load config module tại file .env
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  connectionTimeout: 30000,
  requestTimeout: 30000,
};

let connection;
let isCheck = false;
async function connectToDatabase() {
  if (!connection) {
    try {
      connection = await sql.connect(config);
    } catch (error) {
      console.log(
        getCurrentDateTime() + ` Connect to control_db failed: ${error} `
      );
      //2.2 Send email "ddmmyymmhhss Connect to contril_db failed"
      sendEmail(
        getCurrentDateTime() + ` Connect to control_db failed: ${error} `
      );
    }
  }
}
async function closeConnection() {
  if (connection) {
    await connection.close();
    connection = null;
  }
}
async function queryFileConfig() {
  try {
    const result = await connection.query`SELECT * FROM file_configs`;
    return result.recordset;
  } catch (err) {
    console.log(getCurrentDateTime() + " Query data failed");
    sendEmail(getCurrentDateTime() + " Query data failed");
    return [];
  }
}

async function insertStatusToLog(idConfig, status, note) {
  try {
    const result = await connection.query`
      INSERT INTO file_logs (id_file_config, status_process, note, created_by)
      VALUES (${idConfig}, ${status}, ${note}, ${process.env.USER_MN})
    `;
    return result.rowsAffected;
  } catch (err) {
    console.log(
      getCurrentDateTime() + ` Insert data to file_logs failed ${err}`
    );
    //5.2 Send email thông báo quá trình insert status progess xuống table file_logs thất bại bằng hàm sendEmail()
    sendEmail(getCurrentDateTime() + ` Insert data to file_logs failed ${err}`);
    await closeConnection();
    return;
  }
}
async function updateStatusToLog(idConfig, status, note, date) {
  try {
    const result = await connection.query`
      UPDATE file_logs
      SET status_process = ${status}, note = ${note}
      WHERE FORMAT(create_at, 'dd-MM-yyyy') = ${date} AND id_file_config = ${idConfig}
    `;
    return result.rowsAffected;
  } catch (err) {
    console.log(getCurrentDateTime() + " Update data in file_logs failed");
    await closeConnection();
    return;
  }
}
// nếu chưa có log hoặc crawl thấy bại thì return true else false
async function checkLog(idConfig, date) {
  try {
    const result =
      await connection.query`SELECT status_process from file_logs where FORMAT(create_at, 'dd-MM-yyyy') = ${date} and id_file_config = ${idConfig}`;
    if (
      result.recordset.length === 0 ||
      result.recordset[0]?.status_process.trim() === "Save data failed" ||
      result.recordset[0]?.status_process.trim() === "Crawling data" ||
      result.recordset[0]?.status_process.trim() === "Crawl data failed"
    ) {
      if (
        result.recordset[0]?.status_process.trim() === "Save data failed" ||
        result.recordset[0]?.status_process.trim() === "Crawling data" ||
        result.recordset[0]?.status_process.trim() === "Crawl data failed"
      ) {
        isCheck = true;
      }
      console.log(date + " Crawling data...");
      return true;
    }
    console.log(date + " Crawled the data");
    return false;
  } catch (error) {
    console.log("Error:", error);
    return false;
  }
}

async function scrapeData() {
  let page = 0;
  let allProducts = [];
  //4. Lấy thông tin từ table file_configs bằng hàm queryFileConfig() rồi tiến hành crawl dữ liệu
  const config = await queryFileConfig();
  //4.1 Check xem thử đã crawl dữ liệu vào ngày ddmmyy chưa bằng hàm checkLog()
  if (!(await checkLog(config[0].id, getCurrentDate()))) {
    //4.2 Đóng connect bằng hàm closeConnection()
    await closeConnection();
    return;
  }
  const soucePathOrigin = config[0].source_path;
  const outputFilePath =
    config[0].directory_file +
    "\\" +
    config[0].file_name +
    getCurrentDate() +
    config[0].format;
  if (!isCheck) {
    //5.insert status "Crawling data" bằng hàm insertStatusToLog(..)
    const rows = await insertStatusToLog(
      config[0].id,
      "Crawling data",
      `Crawling data....`
    );
    //5.1 Check quá trình insert
    if (rows <= 0) {
      //5.2 Send email thông báo quá trình insert status progess xuống table file_logs thất bại bằng hàm sendEmail()
      await sendEmail("insert log of failed data crawling process ");
      //5.3 Đóng connect bằng hàm closeConnection()
      await closeConnection();
      return;
    }
  } else {
    await updateStatusToLog(
      config[0].id,
      "Crawling data",
      "Crawling data....",
      getCurrentDate()
    );
  }
  //6 Tiến hành crawl dữ liệu
  //6.1 Check quá trình crawl dữ liệu
  try {
    while (true) {
      const soucePath = soucePathOrigin.replace("{page}", page);
      const response = await axios.get(soucePath);
      const html = response.data;
      const $ = cheerio.load(html);

      const content = $(
        "#__next > main > div.content-w-sidebar > div.sc-e89d1c58-0.gBUlRB.listProduct"
      );
      const productsOnPage = content.find(".sc-e89d1c58-1.kWDRKa");

      if (page === 200) {
        break;
      }

      const productDetailsPromises = [];
      productsOnPage.each((index, element) => {
        const product = {
          name: null,
          price: null,
          image: null,
          manufacturer: null,
          sku: null,
          size: null,
          weight: null,
          chip: null,
          app_popular: null,
          origin: null,
          guarantee: null,
          scanHz: null,
          color: null,
          operating_system: null,
          tech_image: null,
          tech_sound: null,
          resolution: null,
          size_screen: null,
        };
        product.name = $(element).find("a > div > h3").text().trim();
        product.price = $(element)
          .find(".sc-651a3ef7-12.hujRwb > span:nth-child(1)")
          .text();
        product.image = $(element)
          .find(" a > div:nth-child(1) > div.sc-651a3ef7-5.etjVzc > img")
          .attr("src");

        const productLink = $(element).find("a").attr("href");
        if (productLink) {
          const productDetailPromise = getProductDetail(productLink).then(
            (productDetail) => {
              product.manufacturer = productDetail.manufacturer;
              product.sku = productDetail.sku;
              product.size = productDetail.size;
              product.weight = productDetail.weight;
              product.chip = productDetail.chip;
              product.app_popular = productDetail.appPopular;
              product.origin = productDetail.origin;
              product.guarantee = productDetail.guarantee;
              product.scanHz = productDetail.scanHz;
              product.color = productDetail.color;
              product.operating_system = productDetail.operatingSystem;
              product.tech_image = productDetail.techImage;
              product.tech_sound = productDetail.techSound;
              product.resolution = productDetail.resolution;
              product.size_screen = productDetail.sizeScreen;

              return product;
            }
          );
          productDetailsPromises.push(productDetailPromise);
        }
      });

      const productsWithDetails = await Promise.all(productDetailsPromises);
      allProducts = [...allProducts, ...productsWithDetails];
      page = page + 20;
    }
    //7 Send email thông báo crawl được bao nhiêu dòng dữ liệu thành công bằng hàm sendEmail(..)
    await sendEmail(
      getCurrentDateTime() +
        ` ${allProducts.length} lines of data have been downloaded"`
    );
    //8 Tiến hành lưu xuống file có đường dẫn "D:/DataWareHouse/Data/file_name.csv" bằng hàm saveToCsv(...)
    await saveToCSV(config, allProducts, outputFilePath);
  } catch (error) {
    console.log(error);
    //6.2 Send email thông báo crawl dữ liệu thất bại bằng hàm sendEmail() và update status "Crawl data failed" and note là nguyên nhân lỗi
    await sendEmail(`Crawl data failed: ${error}`);
    await updateStatusToLog(
      config[0].id,
      "Crawl data failed",
      `${error}`,
      getCurrentDate()
    );
    //6.3 Đóng connect bằng hàm closeConnection()
    await closeConnection();
    return;
  }
}

async function getProductDetail(productLink) {
  try {
    const response = await axios.get(`${productLink}`);
    const html = response.data;
    const $ = cheerio.load(html);
    const details = {};
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(1) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Hãng sản xuất:"
    ) {
      details.manufacturer = null;
    } else {
      details.manufacturer = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(1) > p > span:nth-child(2)"
      ).text();
    }
    details.sku = $(
      "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(2) > p > span:nth-child(2)"
    ).text();
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(3) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Kích thước (DxRxC):"
    ) {
      details.size = null;
    } else {
      details.size = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(3) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(4) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Trọng lượng:"
    ) {
      details.weight = null;
    } else {
      details.weight = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(4) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(5) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Bộ xử lý:"
    ) {
      details.chip = null;
    } else {
      details.chip = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(5) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(6) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Ứng dụng phổ biến::"
    ) {
      details.appPopular = null;
    } else {
      details.appPopular = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(6) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(7) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Xuất xứ:"
    ) {
      details.origin = null;
    } else {
      details.origin = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(7) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(8) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Bảo hành:"
    ) {
      details.guarantee = null;
    } else {
      details.guarantee = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(8) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(9) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Tần số (Hz):"
    ) {
      details.scanHz = null;
    } else {
      details.scanHz = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(9) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(15) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Màu sắc:"
    ) {
      details.color = null;
    } else {
      details.color = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(15) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(16) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Màn hình hiển thị:"
    ) {
      details.screen = null;
    } else {
      details.screen = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(16) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(18) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Hệ điều hành, giao diện:"
    ) {
      details.operatingSystem = null;
    } else {
      details.operatingSystem = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(18) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(20) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Công nghệ âm thanh:"
    ) {
      details.techSound = null;
    } else {
      details.techSound = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(20) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(21) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Công nghệ xử lý hình ảnh:"
    ) {
      details.techImage = null;
    } else {
      details.techImage = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(21) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(22) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Độ phân giải màn hình:"
    ) {
      details.resolution = null;
    } else {
      details.resolution = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(22) > p > span:nth-child(2)"
      ).text();
    }
    if (
      $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(23) > p > span:nth-child(1)"
      )
        .text()
        .trim() !== "Kích thước màn hình:"
    ) {
      details.sizeScreen = null;
    } else {
      details.sizeScreen = $(
        "#detail-page > div.sc-e0b93fa4-0.lgAAvz > div:nth-child(4) > div > div.sc-74ee4254-2.sc-74ee4254-4.eEsoBN.eyvgKO > div > ul > li > ul > li:nth-child(23) > p > span:nth-child(2)"
      ).text();
    }
    return details;
  } catch (error) {
    console.error("Lỗi khi lấy chi tiết sản phẩm:", error);
    return {};
  }
}

async function saveToCSV(config, data, outputFilePath) {
  //8.1. Check quá trình lưu dữ liệu xuống file
  try {
    const json2csvParser = new Parser({ delimiter: ";" });
    const csv = json2csvParser.parse(data);

    fs.writeFileSync(outputFilePath, "\uFEFF" + csv, { encoding: "utf8" });
    //9 Send email thông báo lưu thành công dữ liệu vào địa chỉ D:\DataWareHouse\Data\Productsdd-mm-yy.csv bằng hàm sendEmail(...)
    await sendEmail(`The data has been saved to the file: ${outputFilePath}`);
    //10 Update status "Save data successfully" vào table file_logs bằng hàm updateStatusToLog(...)
    const rowsAffected = await updateStatusToLog(
      config[0].id,
      "Save data successfully",
      `The data has been saved to the file: ${outputFilePath}`,
      getCurrentDate()
    );
    //10.1. Check quá trình upadte
    if (rowsAffected <= 0) {
      //10.2 Send email thông báo quá trình update status progess xuống table file_logs thất bại bằng hàm sendEmail()
      await sendEmail("update log of failed data file saving process");
      //10.3 Đóng connect bằng hàm closeConnection()
      await closeConnection();
    } else {
      //11 Send email thông báo quá trình update status progess xuống table file_logs thành công bằng hàm sendEmail()
      await sendEmail("update log of successful data file saving process");
      //12 Đóng connect bằng hàm closeConnection()
      await closeConnection();
    }
  } catch (error) {
    //8.2 Send email thông báo lưu dữ liệu thất bại bằng hàm sendEmail(...)
    await sendEmail(`Saving data to file ${outputFilePath} failed: ${error}`);
    //8.3 update status "Save data failed" vào table file_logs bằng hàm updateStatusToLog();
    const rowsAffected = await updateStatusToLog(
      config[0].id,
      "Save data failed",
      `${error}`,
      getCurrentDate()
    );
    //8.3.1 Check quá trình update
    if (rowsAffected <= 0) {
      //8.3.2 Send email thông báo quá trình update status progess xuống table file_logs thất bại bằng hàm sendEmail()
      await sendEmail("update log of failed data file saving process");
      //8.3.3 Đóng connect bằng hàm closeConnection()
      await closeConnection();
    } else {
      //8.4 Send email thông báo quá trình update status progess xuống table file_logs thành công bằng hàm sendEmail()
      await sendEmail("update log of successful data file saving process");
      //8.5 Đóng connect bằng hàm closeConnection()
      await closeConnection();
    }
  }
}
function getCurrentDate() {
  const today = new Date();
  const day = String(today.getDate()).padStart(2, "0");
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const year = today.getFullYear();

  return `${day}-${month}-${year}`;
}
function getCurrentDateTime() {
  const today = new Date();

  const day = String(today.getDate()).padStart(2, "0");
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const year = today.getFullYear();

  const hours = String(today.getHours()).padStart(2, "0");
  const minutes = String(today.getMinutes()).padStart(2, "0");
  const seconds = String(today.getSeconds()).padStart(2, "0");

  return `${day}-${month}-${year} ${hours}:${minutes}:${seconds}`;
}
async function sendEmail(text) {
  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: process.env.MAIL_USER,
      pass: process.env.MAIL_PASSWORD,
    },
  });

  const mailOptions = {
    from: process.env.MAIL_USER,
    to: process.env.MAIL_TO,
    subject: process.env.MAIL_SUBJECT,
    text: text,
  };

  try {
    let info = await transporter.sendMail(mailOptions);
    console.log(text);
  } catch (error) {
    console.error("Error sending email:", error);
  }
}
async function main() {
  try {
    //2. Tại main chạy hàm connectToDataBase() để connect tới control_db
    await connectToDatabase();
    //2.1 Check connect
    if (connection) {
      //3. Chạy hàm scrapeData()
      scrapeData();
    }
  } catch (err) {
    log("connect to database failed");
  }
}
main();
