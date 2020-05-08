data grocery;
infile 'H:\Homework\Project\coffee_groc_1114_1165' firstobs = 2 missover;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR;
run;
proc print data = grocery (obs = 10); run;
/* Product description */
proc import datafile='H:\Homework\Project\prod_coffee.xls' Out = Coffee DBMS = xls replace; 
run; 
proc print data = coffee (obs = 10); run;

data coffee1;
set coffee (RENAME=(L5=Brand L3=Company));
run;
proc print data= coffee1(obs=10); run;
/* creating new column in grocery data for merging purpose*/
data coffee2;
set grocery;
upc_new=cats(of SY GE VEND ITEM);
run;
proc print data= coffee2(obs=10); run;
/* creating new column in product dataset for merging purpose*/
data coffee_prod;
set coffee1;
upc_new=cats(of SY GE VEND ITEM) ;
run;
proc print data= coffee_prod(obs=10); run;
/* sorting grocery data*/
proc sort data= coffee2;
by upc_new;
run;
/* sorting product  data*/
proc sort data=coffee_prod;
by upc_new;
run;
/*merging grocery and prod data*/
data coffee3_prod;
merge coffee2 (IN=aa) coffee_prod(DROP = SY GE VEND ITEM);
by upc_new;
if aa;
run;
data coffee3_prod;
set coffee3_prod;
if Brand = 'HILLS BROS' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS HIGH YIELD' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS PERFECT BALANC' THEN Brand = 'HillsBrothers';
run;
/* ------------------------------------------------- TIME SERIES ANALYSIS --------------------------------------------------------------------- */

DATA panel;
INFILE "H:\Homework\Project\coffee_PANEL_GR_1114_1165.dat"  FIRSTOBS=2 EXPANDTABS;
INPUT PANID    WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
RUN;

proc sort data= coffee3_prod;
by IRI_KEY;
run;
proc sort data= panel;
by IRI_KEY;
run;
/*merging grocery and prod data*/
data TimeSeries;
merge coffee3_prod (IN=aa) panel;
by IRI_KEY;
if aa;
run;
proc print data = TimeSeries (obs=10);
where Brand = 'HillsBrothers';
run;
data TimeSeries1;
set TimeSeries; 
where Brand = 'HillsBrothers';
run;
proc export data=TimeSeries1
outfile='H:\HB_Timeseries.csv' DBMS=csv replace;
run;
proc sql;
CREATE TABLE TS1 AS
    SELECT week,sum(dollars) as dollars
    FROM TimeSeries1
	group by WEEK
	order by WEEK ASC;
QUIT;
PROC PRINT DATA= coffee1;RUN;
proc forecast data=TS1 
	lead=52
	out=ram1;
var dollars;
run;
proc print data=ram1 ;
run;
proc export data=ram1
outfile='H:\HB_forecast_Plots.csv' DBMS=csv replace;
run;
proc means data = ram1; Var dollars; run;

/* Descriptive Model 1: Showing Vending ID which will generate more revenue */ 
data Desc1;
infile 'H:\Homework\Project\coffee_groc_1114_1165' firstobs = 2 missover;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR;
run;

proc sql;
CREATE TABLE vend AS
    SELECT vend,sum(dollars) as dollars
    FROM Desc1
	group by vend
	order by dollars desc;
QUIT;
proc print data = vend (obs=10); run;

proc export data=vend
outfile='H:\vend.csv' DBMS=csv replace;
run;
/* Descriptive model 2 - Showing week that produced the highest revenues */
data Desc2;
infile 'H:\Homework\Project\coffee_groc_1114_1165' firstobs = 2 missover;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR;
run;
proc sql;
CREATE TABLE vend1 AS
    SELECT week,sum(dollars) as dollars
    FROM Desc2
	group by week
	order by dollars desc;
QUIT;
proc print data=vend1 ;run; 
proc export data=vend1
outfile='H:\vend1.csv' DBMS=csv replace;
run;


