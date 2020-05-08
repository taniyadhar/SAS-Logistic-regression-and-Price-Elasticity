Title 'Group 5';
/* grocery dataset */
data grocery; 
infile 'H:\Homework\Project\coffee_groc_1114_1165' firstobs = 2 missover;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR;
run;
proc print data = coffee (obs = 10); run;
/* Product description */
proc import datafile='H:\Homework\Project\prod_coffee.xls' Out = Coffee DBMS = xls replace; 
run; 
proc print data = coffee (obs = 10); run;

/* creating new column in grocery dataset for merging */ 
data coffee_groc;
set grocery;
upc_new=cats(of SY GE VEND ITEM);
run;
data prod;
set coffee;
upc_new=cats(of SY GE VEND ITEM) ;
run;
/* sorting coffee_groc and prod for merging */
proc sort data=coffee_groc;
by upc_new;
run;
proc sort data=prod;
by upc_new;
run;
/* Merging coffe_groc and prod */
data coffee_groc_prod;
merge coffee_groc(DROP = SY GE VEND ITEM IN=aa) prod(DROP = SY GE VEND ITEM UPC);
by upc_new;
if aa;
run;
proc print data=coffee_groc_prod (obs= 6); run;

/* Delivery Stores Location Dataset */
Data location;
infile 'H:\Homework\Project\Delivery_Stores' firstobs = 2;
input IRI_KEY OU$ EST_ACV Market_Name $20-44 Open Clsd MskdName$;
run;
proc print data = location (obs=10); run;

/* Sorting Coffee_groc_prod and location dataset by IRI_KEY */
proc sort data = location;
by IRI_KEY;
run;

proc sort data = coffee_groc_prod;
by IRI_KEY;
run;

/* merging coffee_groc_prod and location */
data coffee_groc_prod_location;
merge coffee_groc_prod (IN=aa) location;
by IRI_KEY;
if aa;
run;
proc print data = coffee_groc_prod_location (obs=6); run;

/* checking for missing data in coffee_groc_prod_location */
/* creating a format to group missing and non-missing values */
proc format;
value $missfmt ' ' = 'Missing' Other = 'Not Missing';
value missfmt . = 'Missing' Other = 'Not Missing';
run;

proc freq data = coffee_groc_prod_location;
format _CHAR_ $missfmt.;
tables _CHAR_ / missing missprint nocum nopercent;
format _NUMERIC_ missfmt.;
tables _NUMERIC_ / missing missprint nocum nopercent;
run;
/* after running this, there is no missing data till now */
/* Reading Panel data*/
DATA panel_gr;
INFILE "H:\Homework\Project\coffee_PANEL_GR_1114_1165.dat"  FIRSTOBS=2 EXPANDTABS;
INPUT PANID    WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
RUN;
PROC PRINT DATA=panel_gr(OBS=10);RUN;

/* Reading demographic file */
proc import datafile= 'H:\Homework\Project\ads_demo.csv'
out = Cust_demo
DBMS = CSV replace; GUESSINGROWS = 60000; getnames=yes; 
run;
proc print data = Cust_demo (obs=10); run;

/* Sorting the panel data and demographic data by PANID */
proc sort data = panel_gr;
by PANID;
run;
proc sort data = Cust_demo;
by PANID;
run;

/* Merging panel and demographic data by PANID */
data panel_demo;
merge panel_gr (IN=aa) cust_demo;
by PANID;
if aa;
run;
/* Checking missing data in Panel_demo file */
proc means data=panel_demo NMISS N; run;

/* Removing columns which are irrelevant and have high number of missing values */
data Final_demo;
set panel_demo(Drop = Panelist_Type MALE_SMOKE FEM_SMOKE Language HISP_FLAG HISP_CAT HH_Head_Race__RACE2_ Microwave_Owned_by_HH market_based_upon_zipcode);
run;
proc print data = final_demo (obs = 10); run;
proc means data = final_demo NMISS N; run;

/* Sorting Final_demo and coffee_groc_prod_location for final merging */
proc sort data = coffee_groc_prod_location;
by Week;
run;
proc sort data = final_demo;
by Week;
run;
/* FINAL MERGING OF ENTIRE DATASET */
data Final_Coffee;
merge coffee_groc_prod_location(IN=aa)final_demo;
by Week;
if aa;
run;
proc print data = final_coffee (obs = 10); run;
/* Dropping some more irrelevant columns from the data */
data Final_Coffee;
set Final_coffee (DROP = L1 L4 Level _STUBSPEC_1440RC);
run;
proc print data = final_coffee (obs=10); run;
/* Creating SAS Dataset */
Libname c "H:\Homework\Project\";
data c.FinalCoffee;
set Final_Coffee; 
run;
proc print data = c.FinalCoffee (obs=10); run;

proc contents data = c.FinalCoffee; run;
proc means data = c.FinalCoffee; run;

/* --------------------------------------------------- DESCRIPTIVE ANALYSIS------------------------------------------------------*/
/* Renaming some column names */
data c.FinalCoffee;
set c.FinalCoffee (RENAME=(L5=Brand L3=Company));
run;
proc print data = c.FinalCoffee (obs=10); run;
/* Combining all the hills brothers brand */
data c.FinalCoffee;
set c.FinalCoffee;
if Brand = 'HILLS BROS' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS HIGH YIELD' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS PERFECT BALANC' THEN Brand = 'HillsBrothers';
run;
/* BRANDS RANKING */
Title 'Brands Rank';
proc means data = c.FinalCoffee sum;
class Brand; 
var Dollars;
output out = dollar_sale SUM = Sum_of_Sales;
run;
proc sort data = dollar_sale;
by descending Sum_of_Sales;
run;
proc print data = dollar_sale;
where _Type_ = 1;
run;
data TopBrands;
set dollar_sale(obs=7);
where _Type_ = 1;
run;
proc sort data = TopBrands; by descending Sum_Of_Sales; 
proc print data = TopBrands(drop = _TYPE_);
run;

/* Create an 8th brand called “Other” that has all other brands that are not in the top 10.*/
data Eight_Brands;
set c.FinalCoffee;
if strip(Brand) not in ('MAXWELL HOUSE','PRIVATE LABEL','FOLGERS','STARBUCKS','FOLGERS COFFEE HOUSE','EIGHT O CLOCK', 'HillsBrothers')
then Brand = 'OTHER';
run;
/* Creating permanent sas dataset for the eight_brands */
Libname e "H:\Homework\Project\";
data e.EightBrands;
set Eight_Brands; 
run;
proc print data=e.EightBrands (obs=10); run;

/* Calculating average price display, features of each of the 8 brands. */
proc freq data = e.EightBrands; table Brand; run;
proc printto log='H:\Homework\Project\upc2_concat.log';
run;
proc print data = e.EightBrands (obs=10); run;
data BrandDetails;
set e.EightBrands;
AvgPPU = (dollars/units*vol_eq);
if d eq 0 then DISPLAY = 0;else DISPLAY = 1;
if f EQ "NONE" then FEATURE = 0;
else FEATURE = 1; 
put DISPLAY FEATURE AvgPPU;
run;
proc printto;
run;
TITLE 'Average Price, Display, Feature';
proc means data = BrandDetails;
class Brand;
Var AvgPPU DISPLAY FEATURE;
run;
proc print data = BrandDetails (obs=10); run;

/* Top 20 Regions in terms of Dollar sales for all brands */ 
Proc means data = e.EightBrands sum;
class Market_Name;
var Dollars;90
output out = region sum = Total_Sales;
run;

proc sort data = region; 
by Descending Total_Sales;
run;
data TopRegions;
set region(obs=20);
where _TYPE_ = 1;
run;
Proc print data = TopRegions(drop = _TYPE_); run;

/* Making a dataset with only HillsBrothers brand */
proc print data = h.HBCoffee (obs=5); run;
/* Checking the number of missing data */
proc means data=h.HBCoffee NMISS N; run;
/* Deleting some more irrelevant columns */
data h.HBCoffee;
set h.HBCoffee(Drop = EXT_FACT HH_Head_Race__RACE3_ Number_of_Dogs Number_of_Cats);
run;
proc print data=h.HBCoffee (obs=6); run;

/* Find average prices, display, features of HillsBros Brand */
proc freq data = h.HBCoffee; table upc_new; run;

proc printto log="H:\Homework\Project\upc3_concat.log";
run;

data h.HBCoffee;
set h.HBCoffee;
AVGPPU = (dollars/units*vol_eq);
if d eq 0 then DISPLAY = 0;else DISPLAY = 1;
if f EQ "NONE" then FEATURE = 0;
else FEATURE = 1; 
put DISPLAY FEATURE AvgPPU;
run;
proc printto; run;

proc print data = h.HBCoffee (obs=6); run;

/* Top 5 regions in terms of dollar sales for HillsBros Brand */

proc means data = H.HBCoffee sum;
class Market_Name;
var Dollars; 
output out=top5_regions SUM = Total_Sales;
run;

proc sort data = top5_regions;
where _TYPE_ = 1;
by descending Total_Sales;
run;

proc print data = top5_regions (obs=5); where _TYPE_ = 1; run;

/* Sales by the flavor scent of HillsBros Brand */

proc means data = H.HBCoffee sum;
class FLAVOR_SCENT;
var Dollars;
output out=TopFlavors SUM = Total_Sales;
run;

proc sort data = TopFlavors;
where _TYPE_ = 1;
by descending Total_Sales;
run;
proc print data = TopFlavors; run;

/*----------------------------------------------------------------PANEL DATA ANALYSIS------------------------------------------------------------------------*/
/* grocery dataset*/
data coffee;
infile 'H:\Homework\5\coffee_groc_1114_1165' firstobs=2 missover;
input IRI_KEY  WEEK SY  GE VEND ITEM UNITS DOLLARS F $ D PR;
run;

/*product description dataset*/
PROC IMPORT DATAFILE='H:\Homework\5\prod_coffee.xls' OUT =coffee1 DBMS=xls replace;
RUN;
/* Renaming the Company and Brand column*/
data coffee2;
set coffee1 (RENAME=(L5=Brand L3=Company));
run;
/* creating new column in grocery data for merging purpose*/
data coffee3;
set coffee;
upc_new=cats(of SY GE VEND ITEM);
run;
/* creating new column in product dataset for merging purpose*/
data coffee_prod;
set coffee2;
upc_new=cats(of SY GE VEND ITEM) ;
run;
/* sorting grocery data*/
proc sort data= coffee3;
by upc_new;
run;
/* sorting product  data*/
proc sort data=coffee_prod;
by upc_new;
run;
/*merging grocery and prod data*/
data coffee3_prod;
merge coffee3 (DROP = SY GE VEND ITEM IN=aa) coffee_prod(DROP = SY GE VEND ITEM UPC);
by upc_new;
if aa;
run;

data coffee3_prod_FE;
set coffee3_prod;
if Brand = 'HILLS BROS' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS HIGH YIELD' THEN Brand = 'HillsBrothers';
if Brand = 'HILLS BROTHERS PERFECT BALANC' THEN Brand = 'HillsBrothers';
run;

Libname F "H:\Homework\Project\";
data F.coffee3_prod_FE;
set coffee3_prod_FE;
run;
proc print data = F.Coffee3_prod_FE (obs=10); run;

/* creating dummy variable for Column feature*/
DATA F.coffee3_prod_FE ;
set F.coffee3_prod_FE;
IF f = 'A+' THEN feat_coupon = 1; 
    ELSE feat_coupon = 0;
  IF f = 'A' THEN feat_large = 1; 
    ELSE feat_large = 0;
 IF f = 'B' THEN feat_medium = 1; 
    ELSE feat_medium = 0;
 IF f = 'C' THEN feat_small = 1; 
    ELSE feat_small = 0;	
 IF f = 'NONE' THEN feat_none = 1; 
    ELSE feat_none = 0;	
RUN;

proc printto log="H:\Homework\Project\upc_concat.log";
run;

data F.coffee3_prod_FE;
set F.coffee3_prod_FE;
AvgPPU = (dollars/units)/(vol_eq*16);
if d = 0 then DISPLAY = 0;else DISPLAY = 1;
if PR = 1 then PriceRed = 1;else PriceRed = 0;
put DISPLAY PriceRed AvgPPU;
run;
proc printto; run;

/* sorting the data and check for missing data and there are no missing values */
proc sort data = F.coffee3_prod_FE;
by IRI_KEY WEEK;
run;
proc means data = F.coffee3_prod_FE NMISS N; run;

/* creating a new table for calculating average value of each varaible */
proc sql;
create table randomeffect as
SELECT IRI_KEY as IRI_KEY, WEEK as WEEK, sum(DOLLARS) as TotalSales, Avg(AvgPPU) as AvgPrice, Avg(DISPLAY)as AvgDisplay, Avg(feat_coupon)as AvgFeat_coupon,
Avg(feat_large)as AvgFeat_large,Avg(feat_medium)as AvgFeat_medium, Avg(feat_small)as AvgFeat_small, Avg(feat_none)as AvgFeat_none, Avg(PriceRed) as AvgPriceRed
FROM F.coffee3_prod_FE
WHERE Brand = 'HillsBrothers'
GROUP BY IRI_KEY,WEEK
ORDER BY IRI_KEY,WEEK; 
quit;
/* running random effect two way model*/
proc panel data=randomeffect plots =None;       
id IRI_KEY WEEK;       
model TotalSales=AvgPrice AvgDisplay AvgFeat_coupon AvgFeat_large AvgFeat_medium AvgFeat_small AvgPriceRed/ rantwo ; 
run;

/* Looking at the Haussman test, p-value<0.05 which means we are rejecting null hypothesis. Hence fixed effect is the best model. */
proc panel data=randomeffect plots =None;       
id IRI_KEY WEEK;       
model TotalSales = AvgPrice AvgDisplay AvgFeat_coupon AvgFeat_large AvgFeat_medium AvgFeat_small AvgPriceRed/  fixtwo ; 
run;

