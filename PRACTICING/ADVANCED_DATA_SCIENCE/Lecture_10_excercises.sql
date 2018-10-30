q -d "," -H -O "select datestamp,variant,engagement_level,gender,count(*) n,sum(ips) ips_sum,sum(ips*ips) ips2_s
um,sum(cps) cps_sum,sum(cps*cps) cps2_sum,sum(pps) pps_sum,sum(pps*pps) pps2_sum,sum(rps) rps_sum,su
m(rps*rps) rps2_sum from sessions-with-features.csv group by datestamp,variant,engagemen_level,gende
r" > sessions-hypercube.csv

wc -l sessions-hypercube.csv
     85 sessions-hypercube.csv

grep "2016-04-12,test,less_engaged,female" sessions-hypercube.csv
2016-04-12,test,less_engaged,female,665,1963,8123,607,1155,134,146,2965,93279