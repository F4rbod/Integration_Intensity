packrat::init("/work/postresearch/Shared/Projects")
library(dplyr)
library(parallel)
library(languageserver)
library(haven)
library(data.table)
options(max.print = 500)
numcores=31
options(width=80)

data_list_test_globus=list.files(path = "/work/postresearch/Shared/Data_raw/Medicare/Claims/Test globus/")
data_list_test_globus=paste("/work/postresearch/Shared/Data_raw/Medicare/Claims/Test globus/", data_list_test_globus, sep="")
test_globus_data= mclapply(X = data_list_test_globus, FUN = read.csv, mc.cores = numcores)

data_list_test_globus

carrier_data_2013 = test_globus_data[[1]]
carrier_data_2014 = test_globus_data[[2]]
carrier_data_2015 = test_globus_data[[3]]
carrier_data_2016 = test_globus_data[[4]]
carrier_data_2017 = test_globus_data[[5]]
carrier_data_2018 = test_globus_data[[6]]
carrier_data_2019 = test_globus_data[[7]]
carrier_data_2020 = test_globus_data[[8]]

colnames_carrier_2013=c("DESY_SORT_KEY","CLAIM_NO","LINE_NUM","CLM_THRU_DT","NCH_CLM_TYPE_CD","CARR_PRFRNG_PIN_NUM","PRF_PHYSN_UPIN","PRF_PHYSN_NPI","ORG_NPI_NUM","CARR_LINE_PRVDR_TYPE_CD","PRVDR_STATE_CD","PRVDR_SPCLTY","PRTCPTNG_IND_CD","CARR_LINE_RDCD_PMT_PHYS_ASTN_C","LINE_SRVC_CNT","LINE_CMS_TYPE_SRVC_CD","LINE_PLACE_OF_SRVC_CD","CARR_LINE_PRCNG_LCLTY_CD","LINE_LAST_EXPNS_DT","HCPCS_CD","HCPCS_1ST_MDFR_CD","HCPCS_2ND_MDFR_CD","BETOS_CD","LINE_NCH_PMT_AMT","LINE_BENE_PMT_AMT","LINE_PRVDR_PMT_AMT","LINE_BENE_PTB_DDCTBL_AMT","LINE_BENE_PRMRY_PYR_CD","LINE_BENE_PRMRY_PYR_PD_AMT","LINE_COINSRNC_AMT","LINE_SBMTD_CHRG_AMT","LINE_ALOWD_CHRG_AMT","LINE_PRCSG_IND_CD","LINE_PMT_80_100_CD","LINE_SERVICE_DEDUCTIBLE","CARR_LINE_MTUS_CNT","CARR_LINE_MTUS_CD","LINE_ICD_DGNS_CD","LINE_ICD_DGNS_VRSN_CD","LINE_HCT_HGB_RSLT_NUM","LINE_HCT_HGB_TYPE_CD","LINE_NDC_CD","CARR_LINE_CLIA_LAB_NUM","CARR_LINE_ANSTHSA_UNIT_CNT")

colnames_carrier_2016=c("DESY_SORT_KEY","CLAIM_NO","LINE_NUM","CLM_THRU_DT","NCH_CLM_TYPE_CD","CARR_PRFRNG_PIN_NUM","PRF_PHYSN_UPIN","PRF_PHYSN_NPI","ORG_NPI_NUM","CARR_LINE_PRVDR_TYPE_CD","PRVDR_STATE_CD","PRVDR_SPCLTY","PRTCPTNG_IND_CD","CARR_LINE_RDCD_PMT_PHYS_ASTN_C","LINE_SRVC_CNT","LINE_CMS_TYPE_SRVC_CD","LINE_PLACE_OF_SRVC_CD","CARR_LINE_PRCNG_LCLTY_CD","LINE_LAST_EXPNS_DT","HCPCS_CD","HCPCS_1ST_MDFR_CD","HCPCS_2ND_MDFR_CD","BETOS_CD","LINE_NCH_PMT_AMT","LINE_BENE_PMT_AMT","LINE_PRVDR_PMT_AMT","LINE_BENE_PTB_DDCTBL_AMT","LINE_BENE_PRMRY_PYR_CD","LINE_BENE_PRMRY_PYR_PD_AMT","LINE_COINSRNC_AMT","LINE_SBMTD_CHRG_AMT","LINE_ALOWD_CHRG_AMT","LINE_PRCSG_IND_CD","LINE_PMT_80_100_CD","LINE_SERVICE_DEDUCTIBLE","CARR_LINE_MTUS_CNT","CARR_LINE_MTUS_CD","LINE_ICD_DGNS_CD","LINE_ICD_DGNS_VRSN_CD","LINE_HCT_HGB_RSLT_NUM","LINE_HCT_HGB_TYPE_CD","LINE_NDC_CD","CARR_LINE_CLIA_LAB_NUM","CARR_LINE_ANSTHSA_UNIT_CNT","CARR_LINE_CL_CHRG_AMT","LINE_OTHR_APLD_IND_CD1","LINE_OTHR_APLD_IND_CD2","LINE_OTHR_APLD_IND_CD3","LINE_OTHR_APLD_IND_CD4","LINE_OTHR_APLD_IND_CD5","LINE_OTHR_APLD_IND_CD6","LINE_OTHR_APLD_IND_CD7","LINE_OTHR_APLD_AMT1","LINE_OTHR_APLD_AMT2","LINE_OTHR_APLD_AMT3","LINE_OTHR_APLD_AMT4","LINE_OTHR_APLD_AMT5","LINE_OTHR_APLD_AMT6","LINE_OTHR_APLD_AMT7","THRPY_CAP_IND_CD1","THRPY_CAP_IND_CD2","THRPY_CAP_IND_CD3","THRPY_CAP_IND_CD4","THRPY_CAP_IND_CD5","CLM_NEXT_GNRTN_ACO_IND_CD1","CLM_NEXT_GNRTN_ACO_IND_CD2","CLM_NEXT_GNRTN_ACO_IND_CD3","CLM_NEXT_GNRTN_ACO_IND_CD4","CLM_NEXT_GNRTN_ACO_IND_CD5")

colnames(carrier_data_2013)=colnames_carrier_2013
colnames(carrier_data_2014)=colnames_carrier_2013
colnames(carrier_data_2015)=colnames_carrier_2013
colnames(carrier_data_2016)=colnames_carrier_2016
colnames(carrier_data_2017)=colnames_carrier_2016
colnames(carrier_data_2018)=colnames_carrier_2016
colnames(carrier_data_2019)=colnames_carrier_2016
colnames(carrier_data_2020)=colnames_carrier_2016

list_to_write=list(carrier_data_2015,
carrier_data_2016,
carrier_data_2017,
carrier_data_2018,
carrier_data_2019,
carrier_data_2020)

names_to_write=list(
"carrier_data_2015.csv",
"carrier_data_2016.csv",
"carrier_data_2017.csv",
"carrier_data_2018.csv",
"carrier_data_2019.csv",
"carrier_data_2020.csv")
#install.packages("fst")
library(fst)
fst::write.fst(carrier_data_2013, "carrier_data_2013.fst")
fst::write.fst(carrier_data_2014, "carrier_data_2014.fst")
fst::write.fst(carrier_data_2015, "carrier_data_2015.fst")
fst::write.fst(carrier_data_2016, "carrier_data_2016.fst")
fst::write.fst(carrier_data_2017, "carrier_data_2017.fst")
fst::write.fst(carrier_data_2018, "carrier_data_2018.fst")
fst::write.fst(carrier_data_2019, "carrier_data_2019.fst")
fst::write.fst(carrier_data_2020, "carrier_data_2020.fst")


#outpatient and inpatient data
data_list_csv=list.files(path = "/work/postresearch/Shared/Data_raw/Medicare/Claims/CSV/")
data_list_csv=paste("/work/postresearch/Shared/Data_raw/Medicare/Claims/CSV/", data_list_csv, sep="")
csv_data= mclapply(X = data_list_csv, FUN = read.csv, mc.cores = numcores)

data_list_csv

inpatient_data_2013 = csv_data[[1]]
inpatient_data_2014 = csv_data[[2]]
inpatient_data_2015 = csv_data[[3]]
inpatient_data_2016 = csv_data[[4]]
inpatient_data_2017 = csv_data[[5]]
inpatient_data_2018 = csv_data[[6]]
inpatient_data_2019 = csv_data[[7]]
inpatient_data_2020 = csv_data[[8]]

colnames_inpatient_2013=read.table("~/inp13.txt")
colnames_inpatient_2014=read.table("~/inp14.txt")
colnames_inpatient_2015=read.table("~/inp15.txt")
colnames_inpatient_2016=read.table("~/inp16.txt")
colnames_inpatient_2017=read.table("~/inp17.txt")
colnames_inpatient_2018=read.table("~/inp18.txt")
colnames_inpatient_2019=read.table("~/inp19.txt")
colnames_inpatient_2020=read.table("~/inp20.txt")

colnames(inpatient_data_2013)=colnames_inpatient_2013$x
colnames(inpatient_data_2014)=colnames_inpatient_2013$x
colnames(inpatient_data_2015)=colnames_inpatient_2013$x
colnames(inpatient_data_2016)=colnames_inpatient_2016$x
colnames(inpatient_data_2017)=colnames_inpatient_2016$x
colnames(inpatient_data_2018)=colnames_inpatient_2016$x
colnames(inpatient_data_2019)=colnames_inpatient_2016$x
colnames(inpatient_data_2020)=colnames_inpatient_2016$x

list_to_write_inp=list(
inpatient_data_2013,
inpatient_data_2014,
inpatient_data_2015,
inpatient_data_2016,
inpatient_data_2017,
inpatient_data_2018,
inpatient_data_2019,
inpatient_data_2020)

names_to_write_inp=list(
"inpatient_data_2013.csv",
"inpatient_data_2014.csv",
"inpatient_data_2015.csv",
"inpatient_data_2016.csv",
"inpatient_data_2017.csv",
"inpatient_data_2018.csv",
"inpatient_data_2019.csv",
"inpatient_data_2020.csv")
#install.packages("fst")
library(fst)
fst::write.fst(inpatient_data_2013, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2013.fst")
fst::write.fst(inpatient_data_2014, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2014.fst")
fst::write.fst(inpatient_data_2015, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2015.fst")
fst::write.fst(inpatient_data_2016, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2016.fst")
fst::write.fst(inpatient_data_2017, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2017.fst")
fst::write.fst(inpatient_data_2018, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2018.fst")
fst::write.fst(inpatient_data_2019, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2019.fst")
fst::write.fst(inpatient_data_2020, "/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2020.fst")


#outpatient
outpatient_data_2013 = test_globus_data[[9]]
outpatient_data_2014 = test_globus_data[[10]]
outpatient_data_2015 = test_globus_data[[11]]
outpatient_data_2016 = test_globus_data[[12]]
outpatient_data_2017 = csv_data[[9]]
outpatient_data_2018 = csv_data[[10]]
outpatient_data_2019 = csv_data[[11]]
outpatient_data_2020 = csv_data[[12]]

colnames_outpatient_2013=read.table("~/out13.txt")
colnames_outpatient_2014=read.table("~/out14.txt")
colnames_outpatient_2015=read.table("~/out15.txt")
colnames_outpatient_2016=read.table("~/out16.txt")
colnames_outpatient_2017=read.table("~/out17.txt")
colnames_outpatient_2018=read.table("~/out18.txt")
colnames_outpatient_2019=read.table("~/out19.txt")
colnames_outpatient_2020=read.table("~/out20.txt")

colnames(outpatient_data_2013)=colnames_outpatient_2013$x
colnames(outpatient_data_2014)=colnames_outpatient_2013$x
colnames(outpatient_data_2015)=colnames_outpatient_2013$x
colnames(outpatient_data_2016)=colnames_outpatient_2016$x
colnames(outpatient_data_2017)=colnames_outpatient_2016$x
colnames(outpatient_data_2018)=colnames_outpatient_2016$x
colnames(outpatient_data_2019)=colnames_outpatient_2016$x
colnames(outpatient_data_2020)=colnames_outpatient_2016$x

list_to_write_inp=list(
outpatient_data_2013,
outpatient_data_2014,
outpatient_data_2015,
outpatient_data_2016,
outpatient_data_2017,
outpatient_data_2018,
outpatient_data_2019,
outpatient_data_2020)

names_to_write_inp=list(
"outpatient_data_2013.csv",
"outpatient_data_2014.csv",
"outpatient_data_2015.csv",
"outpatient_data_2016.csv",
"outpatient_data_2017.csv",
"outpatient_data_2018.csv",
"outpatient_data_2019.csv",
"outpatient_data_2020.csv")
#install.packages("fst")
library(fst)
fst::write.fst(outpatient_data_2013, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2013.fst")
fst::write.fst(outpatient_data_2014, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2014.fst")
fst::write.fst(outpatient_data_2015, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2015.fst")
fst::write.fst(outpatient_data_2016, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2016.fst")
fst::write.fst(outpatient_data_2017, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2017.fst")
fst::write.fst(outpatient_data_2018, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2018.fst")
fst::write.fst(outpatient_data_2019, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2019.fst")
fst::write.fst(outpatient_data_2020, "/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2020.fst")


