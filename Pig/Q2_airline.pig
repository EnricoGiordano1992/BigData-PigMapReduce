RAW_DATA = LOAD '/user/student/PROGETTO/INPUT/2008.csv' USING PigStorage(',') AS
	(year: int, month: int, day: int, dow: int,
	dtime: int, sdtime: int, arrtime: int, satime: int,
	carrier: chararray, fn: int, tn: chararray,
	etime: int, setime: int, airtime: int,
	adelay: int, ddelay: int,
	scode: chararray, dcode: chararray, dist: int,
	tintime: int, touttime: int,
	cancel: chararray, cancelcode: chararray, diverted: int,
	cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

CARRIER_DATA = FOREACH RAW_DATA GENERATE month AS m, carrier AS cname;
GROUP_CARRIERS = GROUP CARRIER_DATA BY (m,cname);
COUNT_CARRIERS = FOREACH GROUP_CARRIERS GENERATE FLATTEN(group), LOG10(COUNT(CARRIER_DATA)) AS popularity;

STORE COUNT_CARRIERS INTO '/user/student/PROGETTO/OUTPUT/COUNT_CARRIERS' USING PigStorage(',');
