/**
 * Format a number to currency
 * @param {number} amount - The amount to format
 * @param {string} locale - The locale code (e.g., "en-US", "vi-VN")
 * @param {string} currency - The currency code (e.g., "USD", "VND")
 * @returns {string} - The formatted currency string
 */
export function formatCurrency(amount, locale = "vi-VN", currency = "VND") {
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: currency,
  }).format(amount);
}
