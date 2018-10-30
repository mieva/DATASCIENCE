q -d "," -H -O "select x.customer_id,datestamp,variant,engagement_level,gender,ips,cps,pps,rps from sessions.csv x left outer join customer-variant.csv y on y.customer_id=x.customer_id left outer join customer-features.csv z on z.customer_id=x.customer_id" > sessions-with-features.csv

q -d "," -H -O "select variant,count(*) n,cast(sum(rps) as float) rps_sum,cast(sum(rps*rps) as float) rps2_sum from sessions-with-features.csv group by variant" | q -d "," -H -O "select variant,n,rps_sum/n rps,(rps2_sum/n - (rps_sum/n)*(rps_sum/n))/n rpsError2 from -"

sqrt(rpsError2) =

q -d "," -H -O "select variant,engagement_level,count(*) n,cast(sum(rps) as float) rps_sum,cast(sum(rps*rps) as float) rps2_sum from sessions-with-features.csv group by variant,engagement_level" | q -d "," -H -O "select variant,engagement_level,n,rps_sum/n rps,(rps2_sum/n - (rps_sum/n)*(rps_sum/n))/n rpsError2 from -"

sqrt(rpsError2) =