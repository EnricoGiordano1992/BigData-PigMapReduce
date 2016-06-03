-- First, we load the raw data from a test dataset
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

A = FOREACH RAW_DATA GENERATE scode AS s, dcode AS d;

B = GROUP A by (s,d);

COUNT = FOREACH B GENERATE group, COUNT(A);

STORE COUNT INTO '/user/student/PROGETTO/OUTPUT/BUSIEST' USING PigStorage(',');
