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

A = FOREACH RAW_DATA GENERATE month AS m, carrier, (int)(arrtime-satime) AS delay;

B = GROUP A BY carrier;

COUNT_TOTAL = FOREACH B {
	C = FILTER A BY (delay >= 15); -- only keep tuples with a delay >= than 15 minutes
	GENERATE group, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS frac;
}

STORE COUNT_TOTAL INTO '/user/student/PROGETTO/OUTPUT/COUNT_TOTAL_DELAYED_CARRIER_15_MIN' USING PigStorage(',');
