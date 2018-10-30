q -d "," -H -O "SELECT COUNT(*) FROM item-features.csv"
COUNT(*)
100

q -d "," -H -
O "SELECT COUNT(*) FROM customer-features.csv"
COUNT(*)
100000

q -d "," -H -
O "SELECT COUNT(*) FROM customer-interactions.csv"
COUNT(*)
973190

q -d "," -H -
O "SELECT COUNT(*) FROM customer-variant.csv"
COUNT(*)
100000