import React from "react";
import "./ItemProduct.css"; // File CSS để style riêng

const ItemProduct = ({ image, name, price }) => {
  return (
    <div className="item-product">
      <img src={image} alt={name} className="item-product-image" />
      <h3 className="item-product-name">{name}</h3>
      <p className="item-product-price">{price}</p>
    </div>
  );
};

export default ItemProduct;
