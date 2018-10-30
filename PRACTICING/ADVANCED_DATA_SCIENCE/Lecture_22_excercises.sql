q -d "," -H -
O "select popularity,customer_id,datestamp,count(distinct x.item_id) ips,sum(clicked) cps,sum(purcha
sed) pps,sum(paidamount) rps from customer-interactions.csv x join item-features-binned.csv y on y.i
tem_id=x.item_id group by popularity,customer_id,datestamp" >popularity-sessions.csv

q -d "," -H -O "select popularity,x.customer_id,datestamp,variant,engagement_level,gender,ips,cps,pps,rps from popularity-sessions.csv x join customer-variant.csv y on y.customer_id=x.customer_id join customer-fea
tures.csv z on z.customer_id=x.customer_id" > popularity-sessions-with-features.csv

q -d "," -H -O "select popularity,datestamp,variant,engagement_level,gender,0 n,sum(ips) ips_sum,sum(ips*ips) ips2sum,sum(cps) cps_sum,sum(cps*cps) cps2sum,sum(pps) pps_sum,sum(pps*pps) pps2sum,sum(rps) rps_sum,su
m(rps*rps) rps2sum from popularity-sessions-with-features.csv group by popularity,datestamp,variant,engagement_level,gender" > popularity-sessions-hypercube.csv

q -d "," -H -O "select quality,customer_id,datestamp,count(distinct x.item_id) ips,sum(clicked) cps,sum(purchased) pps,sum(paidamount) rps from customer-interactions.csv x join item-features-binned.csv y on y.item_id=x.item_id group by quality,customer_id,datestamp" > quality-sessions.csv

q -d "," -H -O "select quality,x.customer_id,datestamp,variant,engagement_level,gender,ips,cps,pps,rps from quality-sessions.csv x join customer-variant.csv y on y.customer_id=x.customer_id join customer-features.csv z on z.customer_id=x.customer_id" > quality-sessions-with-features.csv

q -d "," -H -O "select count(*), sum(ips_sum) from price-sessions-hypercube.csv
count(*),sum(ips_sum)
252,914635