q -d "," -H -
O "select item_id,popularity,case when quality<=3.5 then '0-3.5' when quality<=4.4 then '3.6-4.4' el
se '4.5-up' end quality, case when price<=19 then '0-19' when price<=34 then '20-34' else '35-up' en
d price from item-features.csv" > item-features-binned.csv

q -d "," -H -O "select price,customer_id,datestamp,count(distinct x.item_id) ips,sum(clicked) cps,sum(purchased) pps,sum(paidamount) rps from customer-interactions.csv x join item-features-binned.csv y on y.item_id=x.item_id group by price,customer_id,datestamp" >price-sessions.csv

q -d "," -H -O "select price,x.customer_id,datestamp,variant,engagement_level,gender,ips,cps,pps,rps from price-sessions.csv x join customer-variant.csv y on y.customer_id=x.customer_id join customer-features.csv z on z.customer_id=x.customer_id" > price-sessions-with-features.csv

q -d "," -H -O "select price,datestamp,variant,engagement_level,gender,0 n,sum(ips) ips_sum,sum(ips*ips) ips2sum,sum(cps) cps_sum,sum(cps*cps) cps2sum,sum(pps) pps_sum,sum(pps*pps) pps2sum,sum(rps) rps_sum,sum(rps*rps) rps2sum from price-sessions-with-features.csv group by price,datestamp,variant,engagement_lev
el,gender" > price-sessions-hypercube.csv