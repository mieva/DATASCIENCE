q -d "," -H -
O "select count(*) NumberOfSessions, count(distinct customer_id) NumberOfCustomers,count(distinct da
testamp) NumberOfDates from sessions.csv"
NumberOfSessions,NumberOfCustomers,NumberOfDates
165649,100000,7

q -d "," -H -
O "select count(*) n,cast(sum(ips) as float) ips_sum,cast(sum(ips*ips) as float) ips2_sum from sessi
ons.csv" | q -d "," -H -O "select ips_sum/n ips,(ips2_sum/n - (ips_sum/n)*(ips_sum/n))/n ipsError2 f
rom -"
ips,ipsError2
5.52152442816,0.000218790996581

sqrt(0.000218790996581) = 0.01479

q -d "," -H -
O "select count(*) n,cast(sum(cps) as float) cps_sum,cast(sum(cps*cps) as float) cps2_sum from sessi
ons.csv" | q -d "," -H -O "select cps_sum/n cps,(cps2_sum/n - (cps_sum/n)*(cps_sum/n))/n cpsError2 f
rom -"
cps,cpsError2
1.41269793358,2.29599532497e-05

sqrt(2.29599532497e-05) = 0.00479

q -d "," -H -
O "select count(*) n,cast(sum(pps) as float) pps_sum,cast(sum(pps*pps) as float) pps2_sum from sessi
ons.csv" | q -d "," -H -O "select pps_sum/n pps,(pps2_sum/n - (pps_sum/n)*(pps_sum/n))/n ppsError2 f
rom -"
pps,ppsError2
0.273174000447,2.18106759927e-06

q -d "," -H -O "select count(*) n,cast(sum(rps) as float) rps_sum,cast(sum(rps*rps) as float) rps2_sum from sessi
ons.csv" | q -d "," -H -O "select rps_sum/n rps,(rps2_sum/n - (rps_sum/n)*(rps_sum/n))/n rpsError2 f
rom -"
rps,rpsError2
5.58032345502,0.00113166057318

sqrt(0.00113166057318) = 0.03364