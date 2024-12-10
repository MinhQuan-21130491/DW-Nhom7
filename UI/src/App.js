import { useEffect, useState } from "react";
import axios from "axios";
import ItemProduct from "./ItemProduct";
import "./App.css"; // File CSS để style riêng
import { formatCurrency } from "./FormatCurrency";

function App() {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    // Hàm gọi API
    const fetchProducts = async () => {
      try {
        const response = await axios.get(
          `http://172.20.10.4:5000/api/products`
        );

        console.log(response.data);
        setProducts(response.data);
      } catch (error) {
        console.error("Failed to fetch products:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchProducts();
  }, []);
  console.log(products);
  return (
    <div>
      <h1 style={{ textAlign: "center" }}>Báo cáo giá tivi </h1>
      <div className="contain">
        {products?.map((item, index) => {
          return (
            <ItemProduct
              image={item.image}
              name={item.name}
              price={formatCurrency(item.price)}
            />
          );
        })}
      </div>
    </div>
  );
}

export default App;
