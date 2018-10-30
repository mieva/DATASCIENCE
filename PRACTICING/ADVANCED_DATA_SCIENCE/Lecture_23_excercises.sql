q -d "," -H -O "select 'none' dimension_type, 'TOTAL' dimension_value,* from sessions-hypercube.csv" > sessions-multicube.csv

q -d "," -H -O "select 'popularity' dimension_type,* from popularity-sessions-hypercube.csv" >> sessions-multicube.csv

q -d "," -H -O "select 'quality' dimension_type,* from quality-sessions-hypercube.csv" >> sessions-multicube.csv

q -d "," -H -O "select 'price' dimension_type,* from price-sessions-hypercube.csv" >> sessions-multicube.csv