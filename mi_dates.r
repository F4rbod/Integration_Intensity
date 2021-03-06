setwd("/work/postresearch/Shared/Projects/Farbod")
options(repr.matrix.max.rows=100, repr.matrix.max.cols=300)
options(repr.plot.width = 20, repr.plot.height = 15)

numcores=8

library(tidyverse)
library(parallel)
library(data.table)
library(fst)
library(comorbidity)
library(zeallot)
library(reshape)
library(dtplyr)
library(haven)
library(vroom)
library(dplyr)
`%!in%` = Negate(`%in%`)

setDTthreads(numcores)








#diagnosis codes
#from https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleId=52850&ver=26 and https://www.aapc.com/codes/cpt-codes-range/93451-93533/10
angio_codes=c(93451,93452,93453,93454,93455,93456,93457,93458,93459,93460,93461,93462,93463,93464
              ,93503,93505,93530,93531,93532,93533)
#from https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleid=57326&ver=13&keyword=electrocardiogram&keywordType=starts&areaId=all&docType=NCA,CAL,NCD,MEDCAC,TA,MCD,6,3,5,1,F,P&contractOption=all&sortBy=relevance&bc=1
ecg_codes=c(93000,93005,93010,93040,93041,93042)
#from https://scct.org/page/CardiacCTCodes include CTangio
cardiac_ct_codes=c(75571,75572,75573,75574)
#from https://cardiacmri.com/tech-guide/cpt-codes-relevant-to-cardiac-mri/
cardiac_mri_codes=c(75557,75559,75561,75563,75565)
# from https://medicarepaymentandreimbursement.com/2011/07/cardiovascular-stress-testing-cpt-93015.html and https://www.aapc.com/codes/cpt-codes-range/93000-93050/
stress_test_codes=c(93015,93016,93017,93018)
#from https://www.aapc.com/codes/cpt-codes-range/93303-93356/20     includes stress echo
echocardiography_codes=c(93303,93304,93306,93307,93308,93312,93313,93314,93315,93316,93317,93318
                         ,93320,93321,93325,93350,93351,93356,93352,93355,93356)
#from https://www.aapc.com/codes/cpt-codes-range/92920-92979/ and https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleId=57479#:~:text=CPT%20codes%2092928%2C%2092933%2C%2092929,are%20assigned%20to%20APC%200104.    includes balloon and stent
angioplasty_codes=c(92920,92921,92924,92925,92928,92929,92933,92934,92937,92938,92941,92943,92944
                    ,92973,92974,92975,92978,92979,93571,93572,"C9600","C9601","C9602","C9603"
                    ,"C9604","C9605","C9606","C9607","C9608")
#from https://www.medaxiom.com/clientuploads/webcast_handouts/Coding_for_CABG-Open_Heart_Procedures.pdf and https://www.aapc.com/codes/cpt-codes-range/33016-33999/10    did not include 33517-33530 since these are used in conjunction with 33533-33548 and not alone, did not include 33542,33545,33548 since these are also in conjunction )aneurismectomy and vsd resection
CABG_codes=c(33510,33511,33512,33513,33514,33516,33533,33534,33535,33536)
#from http://www.icd9data.com/2015/Volume1/390-459/430-438/default.htm and https://www.icd10data.com/ICD10CM/Codes/I00-I99/I60-I69/I63-
stroke_icd_9_codes=c(43301,43311,43321,43331,43381,43391,43401,43411,43491)
office_visit_codes=c("99201","99202","99203","99204","99205","99211","99212","99213","99214"
                     ,"99215")
IHD_icd_9_codes=c(410, 411, 412,413,414)
IHD_icd_10_codes=c("I20", "I21", "I22", "I23", "I24", "I25")

non_us_state_codes=c(40,54,56,57,58,59,60,61,62,63,64,65,66,97,98,99)

primary_care_specialty_codes=c("01", "08", "11", "38")
surgery_specialty_codes=c("02","04","14","19","20","24","28","33","34","40","48","77","78","85","91")

#http://www.icd9data.com/2015/Volume1/390-459/401-405/default.htm
#https://www.icd10data.com/ICD10CM/Codes/I00-I99/I10-I16
hypertension_icd_9_codes=c("401","402","403","404","405")
hypertension_icd_10_codes=c("I10","I11","I12","I13","I15","I16")

#http://www.icd9data.com/2014/Volume1/460-519/490-496/default.htm
#https://www.icd10data.com/ICD10CM/Codes/J00-J99/J40-J47
copd_icd_9_codes=c("490","491","492","493","494","495","496")
copd_icd_10_codes=c("J40","J41","J42","J43","J44","J45","J47")

#http://www.icd9data.com/2015/Volume1/240-279/270-279/278/278.htm?__hstc=93424706.cdd51240e438a5219319ce13ccb23860.1648603374124.1648603374124.1648607295327.2&__hssc=93424706.9.1648607295327&__hsfp=908776442
#https://www.icd10data.com/ICD10CM/Codes/E00-E89/E65-E68/E66-
obesity_icd_9_codes=c("278")
obesity_icd_10_codes=c("E66")

#http://www.icd9data.com/2014/Volume1/290-319/295-299/296/default.htm
#https://www.icd10data.com/ICD10CM/Codes/F01-F99/F30-F39
depression_icd_9_codes=c("2962","2963")
depression_icd_10_codes=c("F32","F33")

#http://www.icd9data.com/2015/Volume1/240-279/249-259/default.htm
#https://www.icd10data.com/ICD10CM/Codes/E00-E89/E08-E13
diabetes_icd_9_codes=c("250")
diabetes_icd_10_codes=c("E08","E09","E10","E11","E13")







carrier_data_all_years = read_fst(
    "carrier_data_all_years.fst", as.data.table = T)
head(carrier_data_all_years)
#outpatient_data_all_years = read_fst(
#    "outpatient_data_all_years.fst", as.data.table = T)
#inpatient_data_all_years = read_fst(
#    "inpatient_data_all_years.fst", as.data.table = T)
#
#mbsf_data = read_fst(
#  "/work/postresearch/Shared/Projects/Data_fst/mbsf_data", as.data.table = T)
#revenue_center_outpatient_all_years = read_fst(
#  "/work/postresearch/Shared/Projects/Data_fst/revenue_center_outpatient_all_years.fst", as.data.table = T)
#outpatient_and_revenue_center_data = read_fst(
#  "/work/postresearch/Shared/Projects/Data_fst/outpatient_and_revenue_center_data.fst", as.data.table = T)

#carrier_sample = tail(carrier_data_all_years,100000)
#outpatient_sample = tail(outpatient_data_all_years,100000)
#inpatient_sample = tail(inpatient_data_all_years,100000)
#mbsf_sample = tail(mbsf_data,100000)
#revenue_center_outpatient_sample=tail(revenue_center_outpatient_all_years,100000)
#outpatient_and_revenue_center_data_sample=outpatient_and_revenue_center_data[1:100000]

#head(carrier_sample)
#head(outpatient_sample)
#head(inpatient_sample)
#head(mbsf_sample)
#head(revenue_center_outpatient_sample)
#head(outpatient_and_revenue_center_data_sample)






yearly_calculator_patient_conditions = function(data) {
  
  #requirements
  require(data.table)
  require(dtplyr)
  require(tidyverse)
  require(lubridate)
  
  data %>%
    mutate(
      is_cardiology_related = if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 1) == "I",
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          as.numeric(substr(LINE_ICD_DGNS_CD, 0, 3))>=399 & 
          as.numeric(substr(LINE_ICD_DGNS_CD, 0, 3))<=459 ,NA)),      
      is_catheterization = HCPCS_CD %in% angio_codes,
      is_ecg = HCPCS_CD %in% ecg_codes,
      is_cardiac_ct = HCPCS_CD %in% cardiac_ct_codes,
      is_cardiac_mri = HCPCS_CD %in% cardiac_mri_codes,
      is_stress_test = HCPCS_CD %in% stress_test_codes,
      is_echocardiography = HCPCS_CD %in% echocardiography_codes,
      is_angioplasty = HCPCS_CD %in% angioplasty_codes,
      is_CABG = HCPCS_CD %in% CABG_codes,
      is_stable_angina = ifelse(
        LINE_ICD_DGNS_VRSN_CD == 0,
        LINE_ICD_DGNS_CD %in% c ("I208", "I209"),
        ifelse(LINE_ICD_DGNS_VRSN_CD == 9, LINE_ICD_DGNS_CD == "4139", NA)),
      is_unstable_angina = ifelse(
        LINE_ICD_DGNS_VRSN_CD == 0,
        LINE_ICD_DGNS_CD == "I200",
        ifelse(LINE_ICD_DGNS_VRSN_CD == 9, LINE_ICD_DGNS_CD == "4111", NA)),
      is_MI = if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) == "I21",
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 3) == "410" ,NA)),
      is_cardiac_arrest = if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) == "I46",
        if_else(LINE_ICD_DGNS_VRSN_CD == 9, LINE_ICD_DGNS_CD == "4275", NA)),
      is_stroke = if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) == "I63",
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          LINE_ICD_DGNS_CD %in% stroke_icd_9_codes,NA)),
      is_office_visit = HCPCS_CD %in% office_visit_codes,
      is_cardiology_office_vist =
        (HCPCS_CD %in% office_visit_codes) &
        (PRVDR_SPCLTY %in% c("06", "C3")),
      is_IHD = if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) %in% IHD_icd_10_codes,
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 3) %in% IHD_icd_9_codes,NA)),
      is_by_cardiologist= PRVDR_SPCLTY %in% c("06","C3"),
      is_by_primary_care_physician= PRVDR_SPCLTY %in% primary_care_specialty_codes,
      is_by_surgeon= PRVDR_SPCLTY %in% surgery_specialty_codes,
      is_hypertension= if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) %in% hypertension_icd_10_codes,
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 3) %in% hypertension_icd_9_codes,NA)),
      is_copd= if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) %in% copd_icd_10_codes,
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 3) %in% copd_icd_9_codes,NA)),
      is_obesity= if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) %in% obesity_icd_10_codes,
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 3) %in% obesity_icd_9_codes,NA)),
      is_depression= if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) %in% depression_icd_10_codes,
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 4) %in% depression_icd_9_codes,NA)),
      is_diabetes= if_else(
        LINE_ICD_DGNS_VRSN_CD == 0,
        substr(LINE_ICD_DGNS_CD, 0, 3) %in% diabetes_icd_10_codes,
        if_else(
          LINE_ICD_DGNS_VRSN_CD == 9,
          substr(LINE_ICD_DGNS_CD, 0, 3) %in% diabetes_icd_9_codes,NA))
    ) %>%
    as.data.table()
}

yearly_patient_conditions_carrier=yearly_calculator_patient_conditions(carrier_data_all_years)
head(yearly_patient_conditions_carrier)

MI_date_finder=function(data, time_frame = 365, diagnosis){
  
  data%>%
    group_by(DESY_SORT_KEY) %>%
    filter(sum(eval(parse(
      text = paste("is_", diagnosis, sep = "")
    )), na.rm = T) == T) %>%
    mutate(first_diagnosis = min(date[eval(parse(text = paste("is_", diagnosis, sep = ""))) ==
                                        T]), na.rm = T) %>%
    mutate( had_IHD = (
      date - first_diagnosis < 0 &
        first_diagnosis - date < time_frame &
        is_IHD
    ))%>%
    filter(date - first_diagnosis >= 0 &
             date - first_diagnosis < time_frame &
             had_IHD == F,
           .preserve = T) %>%
    summarise(
      MI_date = head(.[is_MI,date],1)
      )%>%
  as.data.table()
  }
MI_DATES = MI_date_finder(yearly_patient_conditions_carrier , diagnosis = "stable_angina")

write.fst(MI_DATES,"results_apr/MI_DATES.fst")