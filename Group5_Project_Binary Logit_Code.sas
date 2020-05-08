/*********Group5_BinaryLogit_Code***********************************************/
/*********** DATA PREPROCESSING  ***************************************/
/* Reading the grocery dataset */
data grocery;
infile 'H:\Project\coffee_groc_1114_1165' firstobs = 2 missover;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR;
run;
proc print data = grocery (obs = 10); run;

/*  Importing the grocery dataset  */
proc import datafile='H:\Project\prod_coffee.csv' Out = coffee DBMS = csv replace; 
run; 
proc print data = coffee (obs = 10); run;

/* creating new column upc_new in grocery dataset for merging */
data coffee_grocery;
set grocery;
upc_new = cats(of SY GE VEND ITEM);
run;

/* creating new column upc_new in product dataset for merging */
data coffee_prod;
set coffee;
upc_new = cats(of SY GE VEND ITEM);
run;

/* sorting both product and grocery datasets */
proc sort data = coffee_grocery;
by upc_new;
run;
proc sort data = coffee_prod;
by upc_new;
run; 
proc print data = coffee_grocery(obs = 10); run;
proc print data = coffee_prod (obs = 10); run;

/* merging prod and grocery by upc_new */
data coffee_groc_prod;
merge coffee_grocery(DROP = SY GE VEND ITEM IN=aa) coffee_prod(DROP = SY GE VEND ITEM);
by upc_new;
if aa;
run;
proc print data=coffee_groc_prod (obs= 6); run;

proc print data='h:\mergeddata.sas7bdat' (obs= 6); run;

/* Importing mergeddata = grocery dataset(coffee_groc_1114_1165) + prod_coffee */
proc import datafile='H:\mergeddata.csv' Out = mergeddata DBMS = csv replace; 
run; 
proc print data = mergeddata (obs = 10); run;

/* Checking missing data in mergeddata file */
proc means data=mergeddata NMISS N; run;

/*importing the panel_gr data*/
DATA panel_gr;
INFILE "H:\Project\coffee_PANEL_GR_1114_1165.dat"  FIRSTOBS=2 EXPANDTABS;
INPUT PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
RUN;
PROC PRINT DATA=panel_gr(OBS=10);RUN;

/* Sorting the merged data and panel data  by IRIKEY*/
proc sort data = panel_gr;
by IRI_KEY;
run;
proc sort data = mergeddata;
by IRI_KEY;
run;

/*merging merged data (coffee_groc & prod_coffee) + coffee_gr_panel = mergeddata_panel
 Merging merged data and panel data by IRI_KEY */
data mergeddata_panel;
merge mergeddata (IN=aa) panel_gr;
by IRI_KEY;
if aa;
run;

PROC PRINT DATA=mergeddata_panel(OBS=10);RUN;

/* Checking missing data in mergeddata_panel file */
proc means data=mergeddata_panel NMISS N; run;

/*importing the demograhic file*/
proc import datafile= 'H:\Project\ads_demo.csv'
out = demo( RENAME =Panelist_ID=PANID)
DBMS = CSV replace; GUESSINGROWS = 60000; getnames=yes;
run;
proc print data = demo (obs=10); run;

/*sort MERGEDDATA_PANEL  & DEMOGRAPHIC with PANID*/
proc sort data = mergeddata_panel;
by PANID;
run;
proc sort data = demo;
by PANID;
run;

/*Merging MERGEDDATA_PANEL  & DEMOGRAPHIC by PANID */
data mergeddata_panel_demo;
merge mergeddata_panel (IN=aa) demo;
by PANID;
run;

PROC PRINT DATA=mergeddata_panel_demo(OBS=10);RUN;

/* Checking missing data in mergeddata_panel_demo file */
proc means data=mergeddata_panel_demo NMISS N; run;

/* Export SAS dataset and CSV file */
/*merge mergeddata_panel with cust_demo out = mergeddata_panel_demo*/
Libname c "H:\Project_ClassF";
data c.mergeddata_panel_demo;
set mergeddata_panel_demo;
run;
proc print data = c.mergeddata_panel_demo(obs=10); run;

PROC EXPORT DATA=c.mergeddata_panel_demo OUTFILE="H:\Project_ClassF\mergeddata_panel_demo.csv" DBMS= csv REPLACE;
RUN;
/* Renaming some column names  in final dataset*/
data c.finaldata;
set c.mergeddata_panel_demo (RENAME=(L5=Brand L3=Company D=Display F= Feature PR = PriceReduction));
run;
proc print data = c.finaldata (obs=10); run;

/* Importing the final mergeddata_panel_demo and renaming few columns*/
proc import datafile= 'H:\Project_ClassF\mergeddata_panel_demo.csv'
out = FinalDataset( RENAME =(L5=Brand L3=Company D=Display F= Feature PR = PriceReduction))
DBMS = CSV replace; GUESSINGROWS = 60000; getnames=yes;
run;
proc print data = FinalDataset(obs=10); run;

/* Creating a dataset by renaming other brand values to OTHERS */
data OtherBrand;
set FinalDataset;if strip(Brand) not in ('FOLGERS','MAXWELL','PRIVATE','STARBUCK','EIGHT O','CHOCK FU', 'HILLS BR')
then Brand = 'OTHERS';
run;
PROC PRINT DATA = OtherBrand(obs=1000);run;

/* Making it into 3 clusters; Hills bros=1, Top6 = 0, LessMarketShareBrands= 2 */
data Top6_hb_Other;
set OtherBrand;
if Brand = 'HILLS BR' THEN Brand = '1';
if Brand = 'FOLGERS' THEN Brand = '0';
if Brand = 'MAXWELL' THEN Brand = '0';
if Brand = 'PRIVATE' THEN Brand = '0';
if Brand = 'STARBUCK' THEN Brand = '0';
if Brand = 'EIGHT O' THEN Brand = '0';
if Brand = 'CHOCK FU' THEN Brand = '0';
if Brand = 'OTHERS' THEN Brand = '2';
run;

PROC PRINT DATA = Top6_hb_Other(obs=100);run;

/* Exporting the Top6_hb_Other to csv and sas file */
Libname c "H:\Project_ClassF";
data c.Top6_hb_Other;
set Top6_hb_Other;
run;
proc print data = c.Top6_hb_Other(obs=10); run;

PROC EXPORT DATA=c.Top6_hb_Other OUTFILE="H:\Project_ClassF\Top6_hb_Other.csv" DBMS= csv REPLACE;
RUN;

proc import datafile= 'H:\Project_ClassF\Top6_hb_Other.csv'
out = Top6_hb_Other
DBMS = CSV replace; GUESSINGROWS = 60000; getnames=yes;
run;

/* Creating a temporary sas file to include only the Top6 Brand andHills Brother */
data topsix_hb;
set Top6_hb_Other;if Brand = '0' or Brand = '1'; run;

proc print data = topsix_hb(obs=500);run;
proc contents data = topsix_hb;run;
/* 3972928 observations in topsix_hb*/

/**********************************************BINARY LOGIT**********************************/

/* Binary Logit Modeling INCLUDING ALL THE VARIABLES */
proc logistic data = topsix_hb;
class BREWING_METHOD Children_Group_Code Combined_Pre_Tax_Income_of_HH Display FLAVOR_SCENT Family_Size PriceReduction Feature Female_Working_Hour_Code HH_AGE HH_EDU HH_OCC HH_RACE HH_Head_Race__RACE3_ HH_OCC MALE_SMOKE Male_Working_Hour_Code Marital_Status Microwave_Owned_by_HH Number_of_TVs_Hooked_to_Cable Number_of_TVs_Used_by_HH Occupation_Code_of_Female_HH Occupation_Code_of_Male_HH PACKAGE PRODUCT_TYPE Panelist_Type;
model Brand(event='1') = BREWING_METHOD Children_Group_Code Combined_Pre_Tax_Income_of_HH Display Male_Working_Hour_Code Marital_Status Microwave_Owned_by_HH Number_of_TVs_Hooked_to_Cable Number_of_TVs_Used_by_HH Occupation_Code_of_Female_HH Occupation_Code_of_Male_HH PACKAGE PRODUCT_TYPE Panelist_Type PriceReduction UNITS VOL_EQ FLAVOR_SCENT Family_Size MALE_SMOKE Feature DOLLARS Female_Working_Hour_Code HH_AGE HH_EDU HH_OCC HH_RACE HH_Head_Race__RACE3_ HH_OCC;
OUTPUT OUT=top6_hb_out P=PRED_PROB;
run;
proc print data = top6_hb_out(obs=10);run;


/* Binary Logit Modeling INCLUDING ONLY SIGNIFICANT VARIABLES */
proc logistic data = topsix_hb;
class  Children_Group_Code Combined_Pre_Tax_Income_of_HH Display  Family_Size PriceReduction Feature HH_RACE HH_Head_Race__RACE3_ MALE_SMOKE Male_Working_Hour_Code Number_of_TVs_Hooked_to_Cable Number_of_TVs_Used_by_HH Occupation_Code_of_Male_HH Panelist_Type;
model Brand(event='1') =  Children_Group_Code Combined_Pre_Tax_Income_of_HH Display Male_Working_Hour_Code Number_of_TVs_Hooked_to_Cable Number_of_TVs_Used_by_HH Occupation_Code_of_Male_HH Panelist_Type PriceReduction UNITS VOL_EQ Family_Size MALE_SMOKE Feature DOLLARS HH_RACE HH_Head_Race__RACE3_ ;
OUTPUT OUT=top6_hb_out_SIG P=PRED_PROB;
run;
proc print data = top6_hb_out(obs=10);run;

/* Checking for correlation between UNITS, VOL_EQ and DOLLARS */
PROC CORR DATA=topsix_hb;
VAR DOLLARS UNITS VOL_EQ;RUN; /* Units are highly correlated with DOLLARS */


/* Creating dummy variables for the significant features */
data Top6_hb_dummy;
set topsix_hb;
IF Children_Group_Code ='8' THEN No_Children=1; ELSE No_Children=0;
IF Combined_Pre_Tax_Income_of_HH= '1' OR Combined_Pre_Tax_Income_of_HH ='2' THEN HH_Income = 0;
IF Combined_Pre_Tax_Income_of_HH= '5' OR Combined_Pre_Tax_Income_of_HH= '6' THEN HH_Income=1;ELSE HH_Income=2;
IF DISPLAY ='0' THEN NO_DISPLAY = 1; ELSE NO_DISPLAY=0;
IF DISPLAY ='1' THEN MINOR_DISPLAY = 1; ELSE MINOR_DISPLAY=0;
IF DISPLAY='2' THEN MAJOR_DISPLAY=1; ELSE MAJOR_DISPLAY=0;
IF Male_Working_Hour_Code='1' THEN NOT_Employed=1; ELSE NOT_Employed=0;
IF Male_Working_Hour_Code='3' THEN FullTime_MaleWrkr=1;ELSE FullTime_MaleWrkr=0;
IF Male_Working_Hour_Code = '4' THEN Retired_Male =1 ; ELSE Retired_Male =0;
IF Male_Working_Hour_Code = '5' THEN HomeMaker_Male = 1 ; ELSE HomeMaker_Male=0;
IF Number_of_TVs_Hooked_to_Cable= '0' THEN Cable_TV = 0 ; ELSE Cable_TV =1;
IF Family_Size= '0' OR Family_Size = '1' THEN Family_Size_1 = 1 ; ELSE Family_Size_1 =0;
IF Family_Size ='2' THEN Family_Size_2 = 1 ; ELSE Family_Size_2 =0;
IF Family_Size ='3' OR Family_Size = '4' OR Family_Size = '5' OR Family_Size = '6' THEN Family_Size_Gr3= 1 ; ELSE Family_Size_Gr3= 0;
IF Feature = 'A+' THEN Feature_coupon = 1 ; ELSE Feature_coupon = 0;
IF Feature = 'A' THEN Feature_largead = 1 ; ELSE Feature_largead = 0;
IF Feature = 'B' THEN Feature_mediumad = 1 ; ELSE Feature_mediumad = 0;
IF Feature = 'C' THEN Feature_smallad = 1 ; ELSE Feature_smallad = 0;
IF Panelist_Type = '0' THEN Panel_NO = 1 ; ELSE Panel_NO = 0;
IF Panelist_Type = '5' OR Panelist_Type = '6' THEN Panel_YES = 1 ; ELSE Panel_YES = 0;
proc print data = Top6_hb_dummy(obs=10);run; 

/* Drop the original variables FOR WHICH WE CAN CREATED THE DUMMY VARIABLE */
DATA Top6_hb_FINAL(DROP=Panelist_Type Children_Group_Code Combined_Pre_Tax_Income_of_HH DISPLAY Male_Working_Hour_Code Number_of_TVs_Hooked_to_Cable Family_Size Feature );
SET Top6_hb_dummy; RUN;

PROC PRINT DATA=Top6_hb_FINAL(OBS=20);RUN;
PROC CONTENTS DATA = Top6_hb_FINAL; RUN;

/* Binary Logit Modeling INCLUDING ONLY SIGNIFICANT VARIABLES - DUMMY VARIABLES INCLUDED  */
proc logistic data = Top6_hb_FINAL;
class   Cable_TV  Family_Size_1 Family_Size_2  Feature_largead Feature_mediumad  Feature_coupon Feature_smallad  HH_Income   MAJOR_DISPLAY  MALE_SMOKE MINOR_DISPLAY    NOT_Employed     No_Children  Panel_YES PriceReduction  ;
model Brand(event='1') = Cable_TV  Family_Size_1 Family_Size_2 Feature_largead Feature_mediumad  Feature_coupon Feature_smallad   HH_Income  MAJOR_DISPLAY  MALE_SMOKE MINOR_DISPLAY    NOT_Employed    No_Children  Panel_YES PriceReduction VOL_EQ  DOLLARS ;
OUTPUT OUT=top6_hb_out_SIG1 P=PRED_PROB;
run;

/*  To get Confusion Matrix and calculate Hit Ratio */
DATA BL_final;SET top6_hb_out_SIG1;
IF PRED_PROB>0.5 THEN P_final='yes';
IF PRED_PROB<=0.5 THEN P_final='no';
RUN;

proc print data = BL_final (obs=50);run;

proc freq data=BL_final;
tables brand*P_final;
run;
/************************************   ELASTICITY CALCULATION   **********************/

/*OtherBrand contains Top 6 + Hills Bros + OTHERS*/
PROC PRINT DATA = OtherBrand(obs=2000);run;
proc freq data = OtherBrand; table Brand;run;

/* BRANDS RANKING */
proc means data = OtherBrand sum;
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
set dollar_sale(obs=10);
where _Type_ = 1;
run;
proc sort data = TopBrands; by descending Sum_Of_Sales; 
proc print data = TopBrands(drop = _TYPE_);
run;

Title 'Market Share of the Brands';
proc tabulate data = TopBrands;
var Sum_Of_Sales;
class Brand;
	table
	Brand
	all
	,
	Sum_of_Sales * colpctsum;
run;

/*****************We found out here the Avg PPU and Avg Display Values for all the brands********************/
proc printto log="H:\upc2_concat.log";
run;
data Elasticity;
set OtherBrand;
AvgPPU = (dollars/units)/ (vol_eq * 16);
put DISPLAY AvgPPU;
run;
proc printto;
run;

proc means data = Elasticity;
class Brand;
Var AvgPPU DISPLAY ;
run;














