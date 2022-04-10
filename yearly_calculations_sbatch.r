setwd("/work/postresearch/Shared/Projects/Farbod")
options(repr.matrix.max.rows=100, repr.matrix.max.cols=300)
options(repr.plot.width = 20, repr.plot.height = 15)

numcores=90
numcores_foreach=128

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


library(foreach)
library(doMC)
registerDoMC(cores=numcores_foreach)







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

outpatient_data_all_years = read_fst(
    "outpatient_data_all_years.fst", as.data.table = T)
inpatient_data_all_years = read_fst(
    "inpatient_data_all_years.fst", as.data.table = T)

mbsf_data = read_fst(
  "/work/postresearch/Shared/Projects/Data_fst/mbsf_data", as.data.table = T)
revenue_center_outpatient_all_years = read_fst(
  "/work/postresearch/Shared/Projects/Data_fst/revenue_center_outpatient_all_years.fst", as.data.table = T)
outpatient_and_revenue_center_data = read_fst(
  "/work/postresearch/Shared/Projects/Data_fst/outpatient_and_revenue_center_data.fst", as.data.table = T)

carrier_sample = tail(carrier_data_all_years,100000)
outpatient_sample = tail(outpatient_data_all_years,100000)
inpatient_sample = tail(inpatient_data_all_years,100000)
mbsf_sample = tail(mbsf_data,100000)
revenue_center_outpatient_sample=tail(revenue_center_outpatient_all_years,100000)
outpatient_and_revenue_center_data_sample=outpatient_and_revenue_center_data[1:100000]

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

#yearly_patient_conditions_carrier=yearly_calculator_patient_conditions(carrier_data_all_years)
#head(yearly_patient_conditions_carrier)








summarise_expenditures_carrier = function(data, time_frame = 365, diagnosis){
  
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
      first_diagnosis = unique(first_diagnosis),
      tot_allowed_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT),
      office_visit_count = sum(na.rm = T, is_office_visit),
      office_visit_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_office_visit),
      cardiology_visit_count = sum(na.rm = T, is_cardiology_office_vist),
      distinct_clinicians = length(unique(PRF_PHYSN_NPI)),
      distinct_cardiologists = length(.[is_by_cardiologist, unique(PRF_PHYSN_NPI)]),
      distinct_primary_care_physicians = length(.[is_by_primary_care_physician, unique(PRF_PHYSN_NPI)]),
      distinct_surgeons = length(.[is_by_surgeon, unique(PRF_PHYSN_NPI)]),
      distinct_other_specialties= length(.[is_by_surgeon==F &
                                           is_by_cardiologist==F &
                                           is_by_primary_care_physician==F
                                           , unique(PRF_PHYSN_NPI)]),
      catheterization_count = sum(na.rm = T, is_catheterization),
      catheterization_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_catheterization),
      ecg_count = sum(na.rm = T, is_ecg),
      ecg_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_ecg),
      cardiac_ct_count = sum(na.rm = T, is_cardiac_ct),
      cardiac_ct_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_cardiac_ct),
      cardiac_mri_count = sum(na.rm = T, is_cardiac_mri),
      cardiac_mri_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_cardiac_mri),
      stress_test_count = sum(na.rm = T, is_stress_test),
      stress_test_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_stress_test),
      echocardiography_count = sum(na.rm = T, is_echocardiography),
      echocardiography_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_echocardiography),
      angioplasty_count = sum(na.rm = T, is_angioplasty),
      angioplasty_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_angioplasty),
      CABG_count = sum(na.rm = T, is_CABG),
      CABG_cost_carrier = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_CABG),
      stable_angina = sum(is_stable_angina, na.rm = T) > 0,
      unstable_angina = sum(is_unstable_angina, na.rm = T) > 0,
      MI = sum(is_MI, na.rm = T) > 0,
      cardiac_arrest = sum(is_cardiac_arrest, na.rm = T) > 0,
      stroke = sum(is_stroke, na.rm = T) > 0,
      hypertension = sum(is_hypertension, na.rm = T) > 0,
      copd = sum(is_copd, na.rm = T) > 0,
      obesity = sum(is_obesity, na.rm = T) > 0,
      depression = sum(is_depression, na.rm = T) > 0,
      diabetes = sum(is_diabetes, na.rm = T) > 0,
      icd_9_pure = ifelse(prod(LINE_ICD_DGNS_VRSN_CD, na.rm = T) == 0, F, T),
      icd_10_pure = ifelse(sum(LINE_ICD_DGNS_VRSN_CD, na.rm = T) == 0, T, F),
    ) %>%
    group_by(DESY_SORT_KEY) %>%
    mutate(
      year_first_diagnosed=year(first_diagnosis)                                      
    )%>%
    as.data.table()
}


#summary = summarise_expenditures_carrier(yearly_patient_conditions_carrier , diagnosis = "unstable_angina")
#head(summary)








add_cardiology_related_expenditures_carrier = function(data, summary_data, time_frame = 365, diagnosis){
  
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
             had_IHD == F &
             is_cardiology_related,
           .preserve = T) %>%
    summarise(
      tot_allowed_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT),
      office_visit_count_cardiology_related = sum(na.rm = T, is_office_visit),
      office_visit_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_office_visit),
      cardiology_visit_count_cardiology_related = sum(na.rm = T, is_cardiology_office_vist),
      distinct_clinicians_cardiology_related = length(unique(PRF_PHYSN_NPI)),
      distinct_cardiologists_cardiology_related = length(.[is_by_cardiologist, unique(PRF_PHYSN_NPI)]),
      distinct_primary_care_physicians_cardiology_related = length(.[is_by_primary_care_physician, unique(PRF_PHYSN_NPI)]),
      distinct_surgeons_cardiology_related = length(.[is_by_surgeon, unique(PRF_PHYSN_NPI)]),
      distinct_other_specialties_cardiology_related = length(.[is_by_surgeon==F &
                                           is_by_cardiologist==F &
                                           is_by_primary_care_physician==F
                                           , unique(PRF_PHYSN_NPI)]),
      catheterization_count_cardiology_related = sum(na.rm = T, is_catheterization),
      catheterization_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_catheterization),
      ecg_count_cardiology_related = sum(na.rm = T, is_ecg),
      ecg_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_ecg),
      cardiac_ct_count_cardiology_related = sum(na.rm = T, is_cardiac_ct),
      cardiac_ct_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_cardiac_ct),
      cardiac_mri_count_cardiology_related = sum(na.rm = T, is_cardiac_mri),
      cardiac_mri_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_cardiac_mri),
      stress_test_count_cardiology_related = sum(na.rm = T, is_stress_test),
      stress_test_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_stress_test),
      echocardiography_count_cardiology_related = sum(na.rm = T, is_echocardiography),
      echocardiography_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_echocardiography),
      angioplasty_count_cardiology_related = sum(na.rm = T, is_angioplasty),
      angioplasty_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_angioplasty),
      CABG_count_cardiology_related = sum(na.rm = T, is_CABG),
      CABG_cost_carrier_cardiology_related = sum(na.rm = T, LINE_ALOWD_CHRG_AMT * is_CABG),
    )%>%
    left_join(summary_data, . , by = "DESY_SORT_KEY")%>%
    as.data.table()
}


#summary_with_cardiology_related = add_cardiology_related_expenditures_carrier(yearly_patient_conditions_carrier, summary , diagnosis = "unstable_angina")
#head(summary_with_cardiology_related)










outpatient_patient_characteristics=function(outpatient_revenue_center_data){
  
  #requirements
  require(data.table)
  require(dtplyr)
  require(tidyverse)
  require(lubridate)
  
  outpatient_revenue_center_data %>%
    mutate(
      is_office_vist =HCPCS_CD %in% office_visit_codes,
      is_catheterization = HCPCS_CD %in% angio_codes,
      is_ecg = HCPCS_CD %in% ecg_codes,
      is_cardiac_ct = HCPCS_CD %in% cardiac_ct_codes,
      is_cardiac_mri = HCPCS_CD %in% cardiac_mri_codes,
      is_stress_test = HCPCS_CD %in% stress_test_codes,
      is_echocardiography = HCPCS_CD %in% echocardiography_codes,
      is_angioplasty = HCPCS_CD %in% angioplasty_codes,
      is_CABG = HCPCS_CD %in% CABG_codes,
      is_cardiology_related = if_else(
        PRNCPAL_DGNS_VRSN_CD == 0,
        substr(PRNCPAL_DGNS_CD, 0, 1) == "I",
        if_else(
          PRNCPAL_DGNS_VRSN_CD == 9,
          as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))>=399 & 
          as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))<=459 ,NA))
    ) %>%
    as.data.table()
}
                                      
                                      

outpatient_cost_adder=function(outpatient_and_revenue_center_data, summary_data, time_frame=365){
  
  require(tidyverse)
  require(dtplyr)
  require(lubridate)
  data=outpatient_patient_characteristics(outpatient_and_revenue_center_data)%>%as.data.table()
  data=right_join(data,
                  summary_data[,.(DESY_SORT_KEY,first_diagnosis)],by="DESY_SORT_KEY")%>%as.data.table()
  
  result=data%>%
  filter(date - first_diagnosis >= 0 &
         date - first_diagnosis < time_frame 
        ) %>%
  group_by(DESY_SORT_KEY) %>%
  summarise(
    office_visit_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_office_vist),
    catheterization_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_catheterization),
    ecg_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_ecg),
    cardiac_ct_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_cardiac_ct),
    cardiac_mri_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_cardiac_mri),
    stress_test_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_stress_test),
    echocardiography_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_echocardiography),
    angioplasty_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_angioplasty),
    CABG_cost_outpatient = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_CABG),
    office_visit_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_office_vist * is_cardiology_related),
    catheterization_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_catheterization * is_cardiology_related),
    ecg_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_ecg * is_cardiology_related),
    cardiac_ct_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_cardiac_ct * is_cardiology_related),
    cardiac_mri_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_cardiac_mri * is_cardiology_related),
    stress_test_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_stress_test * is_cardiology_related),
    echocardiography_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_echocardiography * is_cardiology_related),
    angioplasty_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_angioplasty * is_cardiology_related),
    CABG_cost_outpatient_cardiology_related = sum(na.rm = T, REV_CNTR_TOT_CHRG_AMT * is_CABG * is_cardiology_related)
    )%>%
  left_join(summary_data,.,by="DESY_SORT_KEY")%>%
  mutate_if(is.double,~replace(., is.na(.), 0))%>%
  mutate(
    office_visit_cost = office_visit_cost_outpatient+office_visit_cost_carrier,
    catheterization_cost = catheterization_cost_outpatient+catheterization_cost_carrier,
    ecg_cost = ecg_cost_outpatient+ecg_cost_carrier,
    cardiac_ct_cost = cardiac_ct_cost_outpatient+cardiac_ct_cost_carrier,
    cardiac_mri_cost = cardiac_mri_cost_outpatient+cardiac_mri_cost_carrier,
    stress_test_cost = stress_test_cost_outpatient+stress_test_cost_carrier,
    echocardiography_cost = echocardiography_cost_outpatient+echocardiography_cost_carrier,
    angioplasty_cost = angioplasty_cost_outpatient+angioplasty_cost_carrier,
    CABG_cost = CABG_cost_outpatient+CABG_cost_carrier,
    office_visit_cost_cardiology_related = office_visit_cost_outpatient_cardiology_related+office_visit_cost_carrier_cardiology_related,
    catheterization_cost_cardiology_related = catheterization_cost_outpatient_cardiology_related+catheterization_cost_carrier_cardiology_related,
    ecg_cost_cardiology_related = ecg_cost_outpatient_cardiology_related+ecg_cost_carrier_cardiology_related,
    cardiac_ct_cost_cardiology_related = cardiac_ct_cost_outpatient_cardiology_related+cardiac_ct_cost_carrier_cardiology_related,
    cardiac_mri_cost_cardiology_related = cardiac_mri_cost_outpatient_cardiology_related+cardiac_mri_cost_carrier_cardiology_related,
    stress_test_cost_cardiology_related = stress_test_cost_outpatient_cardiology_related+stress_test_cost_carrier_cardiology_related,
    echocardiography_cost_cardiology_related = echocardiography_cost_outpatient_cardiology_related+echocardiography_cost_carrier_cardiology_related,
    angioplasty_cost_cardiology_related = angioplasty_cost_outpatient_cardiology_related+angioplasty_cost_carrier_cardiology_related,
    CABG_cost_cardiology_related = CABG_cost_outpatient_cardiology_related+CABG_cost_carrier_cardiology_related
  )%>%
  group_by(DESY_SORT_KEY)%>%
  mutate(
    tot_cheap_prcdr_cost = sum(
        stress_test_cost,
        echocardiography_cost,
        office_visit_cost,na.rm = T),
    tot_expensive_prcdr_cost = sum(
        catheterization_cost,
        cardiac_ct_cost,
        angioplasty_cost
        ,na.rm = T),
    tot_cheap_prcdr_cost_cardiology_related = sum(
        stress_test_cost_cardiology_related,
        echocardiography_cost_cardiology_related,
        office_visit_cost_cardiology_related,na.rm = T),
    tot_expensive_prcdr_cost_cardiology_related = sum(
        catheterization_cost_cardiology_related,
        cardiac_ct_cost_cardiology_related,
        angioplasty_cost_cardiology_related
        ,na.rm = T)
  )%>%
  as.data.table()
  return(result)
}

#summary_with_outpatient=outpatient_cost_adder(outpatient_and_revenue_center_data,summary_with_cardiology_related)
#head(summary_with_outpatient)










twelve_months_after=function(data,colname){

  z=foreach (p = 1:nrow(data), .combine = rbind) %:%
      foreach (a = 0:11, .combine = cbind) %dopar% {
        paste(colname,
              format(data[p,"first_diagnosis"] %m+% months(a),"%m"),"_",
              format(data[p,"first_diagnosis"] %m+% months(a),"%Y"),sep="")
      }
  
  y=foreach(p = 1:nrow(data), .combine = rbind) %:%
      foreach (a = 0:11, .combine = cbind) %dopar% {
        as.numeric(format(data[p,"first_diagnosis"] %m+% months(a),"%Y"))
      }

  return(list(z,y))
}

monthly_characteristics_finder=function(data,colname){
  
  require(dtplyr)
  require(tidyverse)

  z=twelve_months_after(data,colname)[[1]] 
  y=twelve_months_after(data,colname)[[2]] 
  
  result = foreach (p = 1:nrow(data), .combine = rbind) %:%
      foreach (i = 1:12, .combine = cbind) %dopar% {
        if(y[p,i]<2021){
          a=as.character(z[p,i])
          b=data[p,a]
        }
        else{
          b=NA
        }
        b
      }
  return(result)
}


patient_year_of_diagnosis_characteristic_finder = function(data,var_name){
  
  require(dtplyr)
  require(lubridate)
  require(tidyverse)
  
  var=c(paste(var_name,"_",data[,"year_first_diagnosed"],sep=""))
  result=foreach (a = 1:nrow(data),.combine = rbind) %dopar%{
    data[a,var[a]]
    }
  
  return(result)
}

add_patient_characteristics = function(mbsf_data,summary_data){
  
  require(dtplyr)
  require(lubridate)
  require(tidyverse)
  data = left_join(summary_data,mbsf_data,by="DESY_SORT_KEY") %>% as.data.frame()
  
  result=data%>%
    mutate(   
      state_code_at_diagnosis=patient_year_of_diagnosis_characteristic_finder(.,"STATE_CODE"),
      county_code_at_diagnosis=patient_year_of_diagnosis_characteristic_finder(.,"COUNTY_CODE"),
      sex_code_at_diagnosis=patient_year_of_diagnosis_characteristic_finder(.,"SEX_CODE"),
      race_code_at_diagnosis=patient_year_of_diagnosis_characteristic_finder(.,"RACE_CODE"),
      age_at_diagnosis=patient_year_of_diagnosis_characteristic_finder(.,"AGE"),
      ENTITLEMENT_BUY_IN_IND_sum=
      rowSums(monthly_characteristics_finder(data,"ENTITLEMENT_BUY_IN_IND")==3 
              | monthly_characteristics_finder(data,"ENTITLEMENT_BUY_IN_IND")=="C"
              ,na.rm=T),
      HMO_INDICATOR_sum=
      rowSums(monthly_characteristics_finder(data,"HMO_INDICATOR")==0
              ,na.rm=T),
      died_in_one_year_after_diagnosis=date_of_death_collapsed<first_diagnosis+365,
      died_in_two_years_after_diagnosis=date_of_death_collapsed<first_diagnosis+730
          ) %>% 
  as.data.table()
  
  result[is.na(died_in_one_year_after_diagnosis), died_in_one_year_after_diagnosis:=FALSE]
  result[is.na(died_in_two_years_after_diagnosis), died_in_two_years_after_diagnosis:=FALSE]

  result[as.IDate("20191231", "%Y%m%d")<first_diagnosis 
                , died_in_one_year_after_diagnosis:=NA]

  result[as.IDate("20181231", "%Y%m%d")<first_diagnosis 
                , died_in_two_years_after_diagnosis:=NA]
  return(result)
}


#summary_with_patient_characteristics=add_patient_characteristics(mbsf_data,summary_with_outpatient)










add_comorbidity=function(data, summary_data, time_frame = 365){

  require(comorbidity)
  
  #adding comorbidities
  comorbidity_and_phys_data =
    inner_join(data, summary_data[, c("DESY_SORT_KEY",
                                "first_diagnosis",
                                "icd_9_pure",
                                "icd_10_pure")], by = "DESY_SORT_KEY") %>%
    filter(date - first_diagnosis >= 0 &
             date - first_diagnosis < time_frame) %>%
    as.data.table()
  
  
  comorbidity_icd_9 = comorbidity_and_phys_data %>%
    subset(icd_9_pure == T)
  
  if (nrow(comorbidity_icd_9) != 0) {
    comorbidity_icd_9 = comorbidity(
      as.data.table(comorbidity_icd_9),
      id = "DESY_SORT_KEY",
      code = "LINE_ICD_DGNS_CD",
      score = "charlson",
      icd = "icd9",
      assign0 = T
    )
  }
  else {
    comorbidity_icd_9 = data.table(
      DESY_SORT_KEY = NA,
      score = NA,
      index = NA,
      wscore = NA,
      windex = NA
    )
  }
  
  comorbidity_icd_10 = comorbidity_and_phys_data %>%
    subset(icd_10_pure == T)
  
  if (nrow(comorbidity_icd_10) != 0) {
    comorbidity_icd_10 = comorbidity(
      as.data.table(comorbidity_icd_10),
      id = "DESY_SORT_KEY",
      code = "LINE_ICD_DGNS_CD",
      score = "charlson",
      icd = "icd10",
      assign0 = T
    )
  }
  else {
    comorbidity_icd_10 = data.table(
      DESY_SORT_KEY = NA,
      score = NA,
      index = NA,
      wscore = NA,
      windex = NA
    )
  }
 
  comorbidity_all=rbind(
    comorbidity_icd_9[,c("DESY_SORT_KEY","score","index","wscore","windex")]
    ,comorbidity_icd_10[,c("DESY_SORT_KEY","score","index","wscore","windex")]
  )
  result = left_join(summary_data,
                     comorbidity_all,
                     by="DESY_SORT_KEY",) %>% as.data.table()

}

#summary_with_patient_characteristics_comorbidity=add_comorbidity(data = carrier_data_all_years, summary_data = summary_with_patient_characteristics)
#head(summary_with_patient_characteristics_comorbidity)











#adding most common physicians
add_patient_NPI=function(data, summary_data, time_frame = 365){

  comorbidity_and_phys_data =
    inner_join(data, summary_data[, c("DESY_SORT_KEY",
                                "first_diagnosis",
                                "icd_9_pure",
                                "icd_10_pure")], by = "DESY_SORT_KEY") %>%
    filter(date - first_diagnosis >= 0 &
             date - first_diagnosis < time_frame) %>%
    as.data.table()
  
  patient_NPI_count_finder = function(data) {
    result = data %>%
      mutate(is_office_visit = HCPCS_CD %in% office_visit_codes)%>%
      group_by(DESY_SORT_KEY, PRF_PHYSN_NPI) %>%
      summarise(n = sum(is_office_visit,na.rm=T)) %>%
      filter(n>0)%>%
      arrange(.by_group = T, desc(n))
  }
  
  patient_NPI_counts = patient_NPI_count_finder(comorbidity_and_phys_data)
  
  patient_NPI_counts = left_join(patient_NPI_counts,
                                 distinct(data[, .(PRF_PHYSN_NPI, PRVDR_SPCLTY)]), by ="PRF_PHYSN_NPI")
  
  find_most_common = function(data) {
    data %>%
      group_by(DESY_SORT_KEY) %>%
      arrange(.by_group = T, desc(n)) %>%
      slice(1) %>%
      as.data.table()
  }
  
  find_most_common_by_specialty = function(data, specialty_code) {
    data %>%
      filter(PRVDR_SPCLTY %in% specialty_code) %>%
      group_by(DESY_SORT_KEY) %>%
      arrange(.by_group = T, desc(n)) %>%
      slice(1) %>%
      as.data.table()
  }
  
  most_common_physician = find_most_common(patient_NPI_counts)
  #primary care = 01:general practice/ family practice:08/ internal medicine:11/ geriatrics:38
  most_common_primary_care_physician = find_most_common_by_specialty(patient_NPI_counts,
                                                                     specialty_code = c("01", "08", "11", "38"))
  most_common_cardiologists = find_most_common_by_specialty(patient_NPI_counts, specialty_code = c("06","C3"))
  #most_common_interventional_cardiologists = find_most_common_by_specialty(patient_NPI_counts, specialty_code = "C3")
  
  most_common_physician = data.frame(most_common_physician) %>%
    rename_with( ~ paste0("most_common_physician_", .x))
  most_common_primary_care_physician = data.frame(most_common_primary_care_physician) %>%
    rename_with( ~ paste0("most_common_primary_care_physician_", .x))
  most_common_cardiologists = data.frame(most_common_cardiologists) %>%
    rename_with( ~ paste0("most_common_cardiologist_", .x))
  #most_common_interventional_cardiologists = data.frame(most_common_interventional_cardiologists) %>%
   # rename_with( ~ paste0("most_common_interventional_cardiologist_", .x))
  
  summary_data = left_join(
    summary_data,
    most_common_physician,
    by = c("DESY_SORT_KEY" = "most_common_physician_DESY_SORT_KEY")
  )
  summary_data = left_join(
    summary_data,
    most_common_primary_care_physician,
    by = c("DESY_SORT_KEY" = "most_common_primary_care_physician_DESY_SORT_KEY")
  )
  summary_data = left_join(
    summary_data,
    most_common_cardiologists,
    by = c("DESY_SORT_KEY" = "most_common_cardiologist_DESY_SORT_KEY")
  )%>%
  as.data.table()
  
  #summary_data = left_join(
  #  summary_data,
  #  most_common_interventional_cardiologists,
  #  by = c("DESY_SORT_KEY" = "most_common_interventional_cardiologist_DESY_SORT_KEY")
  #)%>%
  #  as.data.table()

  #summary_data[, year_first_diagnosis := lubridate::year(first_diagnosis)]%>%
    #as.data.table()

}

#summary_with_npi=add_patient_NPI(data = carrier_data_all_years, summary_data = summary_with_patient_characteristics_comorbidity)
#head(summary_with_npi)









#adding most common physicians
add_patient_NPI_2013=function(data, summary_data){

  comorbidity_and_phys_data =
    inner_join(data, summary_data[, c("DESY_SORT_KEY",
                                "first_diagnosis",
                                "icd_9_pure",
                                "icd_10_pure")], by = "DESY_SORT_KEY") %>%
    filter(date < as.IDate("2014-01-01")) %>%
    as.data.table()
  
  patient_NPI_count_finder = function(data) {
    result = data %>%
      mutate(is_office_visit = HCPCS_CD %in% office_visit_codes)%>%
      group_by(DESY_SORT_KEY, PRF_PHYSN_NPI) %>%
      summarise(n = sum(is_office_visit,na.rm=T)) %>%
      filter(n>0)%>%
      arrange(.by_group = T, desc(n))
  }
  
  patient_NPI_counts = patient_NPI_count_finder(comorbidity_and_phys_data)
  
  patient_NPI_counts = left_join(patient_NPI_counts,
                                 distinct(data[, .(PRF_PHYSN_NPI, PRVDR_SPCLTY)]), by ="PRF_PHYSN_NPI")
  
  find_most_common = function(data) {
    data %>%
      group_by(DESY_SORT_KEY) %>%
      arrange(.by_group = T, desc(n)) %>%
      slice(1) %>%
      as.data.table()
  }
  
  find_most_common_by_specialty = function(data, specialty_code) {
    data %>%
      filter(PRVDR_SPCLTY %in% specialty_code) %>%
      group_by(DESY_SORT_KEY) %>%
      arrange(.by_group = T, desc(n)) %>%
      slice(1) %>%
      as.data.table()
  }
  
  most_common_physician = find_most_common(patient_NPI_counts)
  #primary care = 01:general practice/ family practice:08/ internal medicine:11/ geriatrics:38
  most_common_primary_care_physician = find_most_common_by_specialty(patient_NPI_counts,
                                                                     specialty_code = c("01", "08", "11", "38"))
  most_common_cardiologists = find_most_common_by_specialty(patient_NPI_counts, specialty_code = c("06","C3"))
  #most_common_interventional_cardiologists = find_most_common_by_specialty(patient_NPI_counts, specialty_code = "C3")
  
  most_common_physician = data.frame(most_common_physician) %>%
    rename_with( ~ paste0("most_common_physician_2013_", .x))
  most_common_primary_care_physician = data.frame(most_common_primary_care_physician) %>%
    rename_with( ~ paste0("most_common_primary_care_physician_2013_", .x))
  most_common_cardiologists = data.frame(most_common_cardiologists) %>%
    rename_with( ~ paste0("most_common_cardiologist_2013_", .x))
  #most_common_interventional_cardiologists = data.frame(most_common_interventional_cardiologists) %>%
   # rename_with( ~ paste0("most_common_interventional_cardiologist_", .x))
  
  summary_data = left_join(
    summary_data,
    most_common_physician,
    by = c("DESY_SORT_KEY" = "most_common_physician_2013_DESY_SORT_KEY")
  )
  summary_data = left_join(
    summary_data,
    most_common_primary_care_physician,
    by = c("DESY_SORT_KEY" = "most_common_primary_care_physician_2013_DESY_SORT_KEY")
  )
  summary_data = left_join(
    summary_data,
    most_common_cardiologists,
    by = c("DESY_SORT_KEY" = "most_common_cardiologist_2013_DESY_SORT_KEY")
  )%>%
  as.data.table()
  
  #summary_data = left_join(
  #  summary_data,
  #  most_common_interventional_cardiologists,
  #  by = c("DESY_SORT_KEY" = "most_common_interventional_cardiologist_DESY_SORT_KEY")
  #)%>%
  #  as.data.table()

  #summary_data[, year_first_diagnosis := lubridate::year(first_diagnosis)]%>%
    #as.data.table()

}

#summary_with_npi_and_2013_npi=add_patient_NPI_2013(data = carrier_data_all_years, summary_data = summary_with_npi)
#head(summary_with_npi_and_2013_npi)










#combining all results
yearly_calculator = function(data,mbsf_data,revenue_center_outpatient_data,diagnosis){

  require(tidyverse)
  require(lubridate)
  require(dtplyr)

  yearly_patient_conditions_carrier =
  data %>%
  yearly_calculator_patient_conditions()%>%
  as.data.table()
  
  result = 
  summarise_expenditures_carrier(yearly_patient_conditions_carrier , diagnosis = diagnosis) %>%
  add_cardiology_related_expenditures_carrier(yearly_patient_conditions_carrier, summary_data = . , diagnosis = diagnosis)%>%
  outpatient_cost_adder(revenue_center_outpatient_data, summary_data = .)%>%
  add_patient_characteristics(mbsf_data = mbsf_data, summary_data = .)%>%
  add_comorbidity(data = data, summary_data = .) %>%
  add_patient_NPI(data = data, summary_data = .) %>%
  add_patient_NPI_2013(data=data , summary_data = .)%>%
  as.data.table()
  
  return(result)

}


yearly_calcualtions_carrier_stable_angina=
yearly_calculator(data=carrier_data_all_years,
                  mbsf_data=mbsf_data,
                  revenue_center_outpatient_data=outpatient_and_revenue_center_data,
                  diagnosis="stable_angina")

write_fst(yearly_calcualtions_carrier_stable_angina,
          "results_apr/yearly_calcualtions_carrier_stable_angina.fst")


yearly_calcualtions_carrier_unstable_angina=
yearly_calculator(data=carrier_data_all_years,
                  mbsf_data=mbsf_data,
                  revenue_center_outpatient_data=outpatient_and_revenue_center_data,
                  diagnosis="unstable_angina")

write_fst(yearly_calcualtions_carrier_unstable_angina,
          "results_apr/yearly_calcualtions_carrier_unstable_angina.fst")









outpatient_data_all_years_stable_angina=inner_join(outpatient_data_all_years, yearly_calcualtions_carrier_stable_angina,by="DESY_SORT_KEY")%>%as.data.table()
intpatient_data_all_years_stable_angina=inner_join(inpatient_data_all_years, yearly_calcualtions_carrier_stable_angina,by="DESY_SORT_KEY")%>%as.data.table()

outpatient_data_all_years_unstable_angina=inner_join(outpatient_data_all_years, yearly_calcualtions_carrier_unstable_angina,by="DESY_SORT_KEY")%>%as.data.table()
intpatient_data_all_years_unstable_angina=inner_join(inpatient_data_all_years, yearly_calcualtions_carrier_unstable_angina,by="DESY_SORT_KEY")%>%as.data.table()


#outpatient
yearly_tot_outpatient=function(data,time_frame=365){
  data%>%
  mutate(is_cardiology_related = if_else(
    PRNCPAL_DGNS_VRSN_CD == 0,
    substr(PRNCPAL_DGNS_CD, 0, 1) == "I",
    if_else(
      PRNCPAL_DGNS_VRSN_CD == 9,
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))>=399 & 
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))<=459 ,NA)))%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_outpatient=sum(CLM_TOT_CHRG_AMT))%>%
    as.data.table()
}

yearly_tot_outpatient_cardiology_related=function(data,time_frame=365){
  data%>%
  mutate(is_cardiology_related = if_else(
    PRNCPAL_DGNS_VRSN_CD == 0,
    substr(PRNCPAL_DGNS_CD, 0, 1) == "I",
    if_else(
      PRNCPAL_DGNS_VRSN_CD == 9,
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))>=399 & 
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))<=459 ,NA)))%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame & is_cardiology_related)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_outpatient_cardiology_related=sum(CLM_TOT_CHRG_AMT))%>%
    as.data.table()
}

#inpatient
yearly_calculator_inpatient=function (data, time_frame=365){
  data %>%
  mutate(is_cardiology_related = if_else(
    PRNCPAL_DGNS_VRSN_CD == 0,
    substr(PRNCPAL_DGNS_CD, 0, 1) == "I",
    if_else(
      PRNCPAL_DGNS_VRSN_CD == 9,
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))>=399 & 
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))<=459 ,NA)))%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_inpatient=sum(CLM_TOT_CHRG_AMT),
              number_of_hospitalizations=length(unique(date)[is.na(CLM_DRG_CD)==F]),
    ) %>%
    mutate(was_hospitalized=number_of_hospitalizations>0)%>%
    as.data.table()
}

#inpatient
yearly_calculator_inpatient_cardiology_related=function (data, time_frame=365){
  data %>%
  mutate(is_cardiology_related = if_else(
    PRNCPAL_DGNS_VRSN_CD == 0,
    substr(PRNCPAL_DGNS_CD, 0, 1) == "I",
    if_else(
      PRNCPAL_DGNS_VRSN_CD == 9,
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))>=399 & 
      as.numeric(substr(PRNCPAL_DGNS_CD, 0, 3))<=459 ,NA)))%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame & is_cardiology_related)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_inpatient_cardiology_related=sum(CLM_TOT_CHRG_AMT),
              number_of_hospitalizations_cardiology_related=length(unique(date)[is.na(CLM_DRG_CD)==F]),
    ) %>%
    mutate(was_hospitalized_cardiology_related=number_of_hospitalizations_cardiology_related>0)%>%
    as.data.table()
}



outpatient_tot_yearly_stable_angina=yearly_tot_outpatient(outpatient_data_all_years_stable_angina)
inpatient_tot_yearly_stable_angina=yearly_calculator_inpatient(intpatient_data_all_years_stable_angina)

outpatient_tot_yearly_stable_angina_cardiology_related=
yearly_tot_outpatient_cardiology_related(outpatient_data_all_years_stable_angina)
inpatient_tot_yearly_stable_angina_cardiology_related=
yearly_calculator_inpatient_cardiology_related(intpatient_data_all_years_stable_angina)


outpatient_tot_yearly_unstable_angina=yearly_tot_outpatient(outpatient_data_all_years_unstable_angina)
inpatient_tot_yearly_unstable_angina=yearly_calculator_inpatient(intpatient_data_all_years_unstable_angina)

outpatient_tot_yearly_unstable_angina_cardiology_related=
yearly_tot_outpatient_cardiology_related(outpatient_data_all_years_unstable_angina)
inpatient_tot_yearly_unstable_angina_cardiology_related=
yearly_calculator_inpatient_cardiology_related(intpatient_data_all_years_unstable_angina)












#stable angina

yearly_calculations_stable_angina=left_join(yearly_calcualtions_carrier_stable_angina, outpatient_tot_yearly_stable_angina, by="DESY_SORT_KEY")%>%as.data.table()
yearly_calculations_stable_angina=left_join(yearly_calculations_stable_angina, inpatient_tot_yearly_stable_angina, by="DESY_SORT_KEY")%>%as.data.table()

yearly_calculations_stable_angina=
left_join(yearly_calculations_stable_angina, outpatient_tot_yearly_stable_angina_cardiology_related, by="DESY_SORT_KEY")%>%as.data.table()
yearly_calculations_stable_angina=
left_join(yearly_calculations_stable_angina, inpatient_tot_yearly_stable_angina_cardiology_related, by="DESY_SORT_KEY")%>%as.data.table()

#finding the total expenditure in one year
yearly_calculations_stable_angina[,total_exp:=sum(tot_allowed_carrier,tot_allowed_outpatient,tot_allowed_inpatient,na.rm = T),by=DESY_SORT_KEY]
yearly_calculations_stable_angina[is.na(number_of_hospitalizations)==T,`:=`(number_of_hospitalizations=0,was_hospitalized=0)]
yearly_calculations_stable_angina[is.na(tot_allowed_outpatient)==T,`:=`(tot_allowed_outpatient=0)]
yearly_calculations_stable_angina[is.na(tot_allowed_inpatient)==T,`:=`(tot_allowed_inpatient=0)]

yearly_calculations_stable_angina[,total_exp_cardiology_related:=sum(tot_allowed_carrier_cardiology_related,tot_allowed_outpatient_cardiology_related,tot_allowed_inpatient_cardiology_related,na.rm = T),by=DESY_SORT_KEY]
yearly_calculations_stable_angina[is.na(number_of_hospitalizations_cardiology_related)==T,`:=`(number_of_hospitalizations_cardiology_related=0,was_hospitalized_cardiology_related=0)]
yearly_calculations_stable_angina[is.na(tot_allowed_outpatient_cardiology_related)==T,`:=`(tot_allowed_outpatient_cardiology_related=0)]
yearly_calculations_stable_angina[is.na(tot_allowed_inpatient_cardiology_related)==T,`:=`(tot_allowed_inpatient_cardiology_related=0)]




#unstable angina

yearly_calculations_unstable_angina=left_join(yearly_calcualtions_carrier_unstable_angina, outpatient_tot_yearly_unstable_angina, by="DESY_SORT_KEY")%>%as.data.table()
yearly_calculations_unstable_angina=left_join(yearly_calculations_unstable_angina, inpatient_tot_yearly_unstable_angina, by="DESY_SORT_KEY")%>%as.data.table()

yearly_calculations_unstable_angina=
left_join(yearly_calculations_unstable_angina, outpatient_tot_yearly_unstable_angina_cardiology_related, by="DESY_SORT_KEY")%>%as.data.table()
yearly_calculations_unstable_angina=
left_join(yearly_calculations_unstable_angina, inpatient_tot_yearly_unstable_angina_cardiology_related, by="DESY_SORT_KEY")%>%as.data.table()

#finding the total expenditure in one year
yearly_calculations_unstable_angina[,total_exp:=sum(tot_allowed_carrier,tot_allowed_outpatient,tot_allowed_inpatient,na.rm = T),by=DESY_SORT_KEY]
yearly_calculations_unstable_angina[is.na(number_of_hospitalizations)==T,`:=`(number_of_hospitalizations=0,was_hospitalized=0)]
yearly_calculations_unstable_angina[is.na(tot_allowed_outpatient)==T,`:=`(tot_allowed_outpatient=0)]
yearly_calculations_unstable_angina[is.na(tot_allowed_inpatient)==T,`:=`(tot_allowed_inpatient=0)]

yearly_calculations_unstable_angina[,total_exp_cardiology_related:=sum(tot_allowed_carrier_cardiology_related,tot_allowed_outpatient_cardiology_related,tot_allowed_inpatient_cardiology_related,na.rm = T),by=DESY_SORT_KEY]
yearly_calculations_unstable_angina[is.na(number_of_hospitalizations_cardiology_related)==T,`:=`(number_of_hospitalizations_cardiology_related=0,was_hospitalized_cardiology_related=0)]
yearly_calculations_unstable_angina[is.na(tot_allowed_outpatient_cardiology_related)==T,`:=`(tot_allowed_outpatient_cardiology_related=0)]
yearly_calculations_unstable_angina[is.na(tot_allowed_inpatient_cardiology_related)==T,`:=`(tot_allowed_inpatient_cardiology_related=0)]












exclusive_hospital_code_finder = function (data,
                                           threshold=0.05,
                                           integrated_place_of_service_codes = c("19", "22"),
                                           all_place_of_service_codes = c("11", "19", "22")){
  require(dtplyr)
  require(tidyverse)
  
  result = data %>%
  filter(LINE_PLACE_OF_SRVC_CD %in% all_place_of_service_codes)%>%
  group_by(HCPCS_CD) %>%
  summarise(prp_in_facility = nrow(.[LINE_PLACE_OF_SRVC_CD %in% integrated_place_of_service_codes])/n()
           )%>%
  as.data.table
  
  exclusive_codes = result[prp_in_facility<threshold | prp_in_facility>(1-threshold),HCPCS_CD]
  

  return(exclusive_codes)
  
}

exclusive_hospital_codes=exclusive_hospital_code_finder(carrier_data_all_years)









#calculate and add physician integration data
#this only uses visits to see if a physician is integrated or not (codde list)

physician_integration_finder = function(data,
                                        integrated_place_of_service_codes = c("19", "22"),
                                        all_place_of_service_codes = c("11", "19", "22"),
                                        #integration_threshold = 0.5,
                                        office_code_list = c(
                                          "99201",
                                          "99202",
                                          "99203",
                                          "99204",
                                          "99205",
                                          "99211",
                                          "99212",
                                          "99213",
                                          "99214",
                                          "99215"
                                        ),
                                       exclusive_hospital_codes) {
  require(dtplyr)
  require(tidyverse)
  
  #data = subset(data, HCPCS_CD %in% code_list)
  result = data %>%
  mutate(
    is_facility = LINE_PLACE_OF_SRVC_CD %in% integrated_place_of_service_codes,
    is_all = LINE_PLACE_OF_SRVC_CD %in% all_place_of_service_codes,
    is_office_visit = HCPCS_CD %in% office_code_list,
    has_non_exclusive_code = HCPCS_CD %!in% exclusive_hospital_codes
  ) %>%
  group_by(PRF_PHYSN_NPI, month_year) %>%
  summarise(
    in_facility_visits_count = sum(is_facility*is_office_visit, na.rm = T),
    in_all_visits_count = sum(is_all*is_office_visit, na.rm = T),
    in_facility_non_exclusive_HCPCS_count = sum(is_facility*has_non_exclusive_code, na.rm = T),
    in_all_non_exclusive_HCPCS_count = sum(is_all*has_non_exclusive_code, na.rm = T),
    in_facility_count = sum(is_facility, na.rm = T),
    in_all_count = sum(is_all, na.rm = T)
    #tot = n(),
  ) %>%
  mutate(
    date = as.IDate(paste(month_year,"-01",sep="")),
    in_facility_visits_prp = in_facility_visits_count / in_all_visits_count,
    in_facility_non_exclusive_HCPCS_prp = in_facility_non_exclusive_HCPCS_count / in_all_non_exclusive_HCPCS_count,    
    in_facility_prp = in_facility_count / in_all_count
    #in_facility_prp_from_tot = in_facility_count / tot
    #is_integrated = in_facility_prp >= integration_threshold,
    #is_integrated_from_tot = in_facility_prp_from_tot >= integration_threshold,
  )%>%
  as.data.table()
  #result=result[,.(PRF_PHYSN_NPI,month_year,is_integrated)]
  #result=reshape(result, idvar = "PRF_PHYSN_NPI", timevar = "month_year", direction = "wide")
  #setcolorder(result,order(colnames(result)))
  #setcolorder(result,"PRF_PHYSN_NPI")  
}

physician_integration_stats = physician_integration_finder(carrier_data_all_years,exclusive_hospital_codes=exclusive_hospital_codes)









#rename columns
rename_last = function(data, how_many, new_names) {
  total_cols = ncol(data)
  setnames(data, (total_cols - how_many + 1):(total_cols), new_names)
}
add_integration_status=function(data, physician_integration_stats){
  
  data=data[,c("DESY_SORT_KEY"	
              ,"first_diagnosis" 
              ,"most_common_physician_PRF_PHYSN_NPI"
              ,"most_common_primary_care_physician_PRF_PHYSN_NPI"
              ,"most_common_cardiologist_PRF_PHYSN_NPI"
              #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
              )]
  
  most_common_physician = left_join(
    data,
    physician_integration_stats,
    by = c(
      "most_common_physician_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  ) %>% as.data.table()
  rename_last(
    most_common_physician,
    ncol(physician_integration_stats)-1,
    paste("most_common_physician_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
    )
  most_common_physician=most_common_physician[,-c("most_common_primary_care_physician_PRF_PHYSN_NPI"
                                                  ,"most_common_cardiologist_PRF_PHYSN_NPI"
                                                  #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
                                                 )]
    
    
  most_common_primary_care = left_join(
    data,
    physician_integration_stats,
    by = c(
      "most_common_primary_care_physician_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  ) %>% as.data.table()
  rename_last(
    most_common_primary_care,
    ncol(physician_integration_stats)-1,
    paste("most_common_primary_care_physician_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
    )
  most_common_primary_care=most_common_primary_care[,-c("most_common_physician_PRF_PHYSN_NPI"
                                                  ,"most_common_cardiologist_PRF_PHYSN_NPI"
                                                  #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
                                                       )]  
  
  most_common_cardiologist = left_join(
    data,
    physician_integration_stats,
    by = c(
      "most_common_cardiologist_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  ) %>% as.data.table()
  rename_last(
    most_common_cardiologist,
    ncol(physician_integration_stats)-1,
    paste("most_common_cardiologist_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
    )
  most_common_cardiologist=most_common_cardiologist[,-c("most_common_physician_PRF_PHYSN_NPI"
                                                  ,"most_common_primary_care_physician_PRF_PHYSN_NPI"
                                                  #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
                                                       )]
  
  #most_common_interventional_cardiologist = left_join(
  #  data,
  #  physician_integration_stats,
  #  by = c(
  #    "most_common_interventional_cardiologist_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  #) %>% as.data.table()
  #rename_last(
  #  most_common_interventional_cardiologist,
  #  ncol(physician_integration_stats)-1,
  #  paste("most_common_interventional_cardiologist_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
  #  )
  #most_common_interventional_cardiologist=most_common_interventional_cardiologist[,-c("most_common_physician_PRF_PHYSN_NPI"
  #                                                ,"most_common_primary_care_physician_PRF_PHYSN_NPI"
  #                                                ,"most_common_cardiologist_PRF_PHYSN_NPI")]
  
  return(list(most_common_physician,
              most_common_primary_care,
              most_common_cardiologist
              #most_common_interventional_cardiologist
             ))
}

melted_physician_integration_stats_stable_angina=add_integration_status(
  data = yearly_calculations_stable_angina,
  physician_integration_stats = physician_integration_stats)

melted_physician_integration_stats_unstable_angina=add_integration_status(
  data = yearly_calculations_unstable_angina,
  physician_integration_stats = physician_integration_stats)









#rename columns
rename_last = function(data, how_many, new_names) {
  total_cols = ncol(data)
  setnames(data, (total_cols - how_many + 1):(total_cols), new_names)
}
add_integration_status_2013=function(data, physician_integration_stats){
  
  data=data[,c("DESY_SORT_KEY"
              ,"first_diagnosis" 
              ,"most_common_physician_2013_PRF_PHYSN_NPI"
              ,"most_common_primary_care_physician_2013_PRF_PHYSN_NPI"
              ,"most_common_cardiologist_2013_PRF_PHYSN_NPI"
              #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
              )]
  
  most_common_physician = left_join(
    data,
    physician_integration_stats,
    by = c(
      "most_common_physician_2013_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  ) %>% as.data.table()
  rename_last(
    most_common_physician,
    ncol(physician_integration_stats)-1,
    paste("most_common_physician_2013_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
    )
  most_common_physician=most_common_physician[,-c("most_common_primary_care_physician_2013_PRF_PHYSN_NPI"
                                                  ,"most_common_cardiologist_2013_PRF_PHYSN_NPI"
                                                  #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
                                                 )]
    
    
  most_common_primary_care = left_join(
    data,
    physician_integration_stats,
    by = c(
      "most_common_primary_care_physician_2013_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  ) %>% as.data.table()
  rename_last(
    most_common_primary_care,
    ncol(physician_integration_stats)-1,
    paste("most_common_primary_care_physician_2013_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
    )
  most_common_primary_care=most_common_primary_care[,-c("most_common_physician_2013_PRF_PHYSN_NPI"
                                                  ,"most_common_cardiologist_2013_PRF_PHYSN_NPI"
                                                  #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
                                                       )]  
  
  most_common_cardiologist = left_join(
    data,
    physician_integration_stats,
    by = c(
      "most_common_cardiologist_2013_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  ) %>% as.data.table()
  rename_last(
    most_common_cardiologist,
    ncol(physician_integration_stats)-1,
    paste("most_common_cardiologist_2013_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
    )
  most_common_cardiologist=most_common_cardiologist[,-c("most_common_physician_2013_PRF_PHYSN_NPI"
                                                  ,"most_common_primary_care_physician_2013_PRF_PHYSN_NPI"
                                                  #,"most_common_interventional_cardiologist_PRF_PHYSN_NPI"
                                                       )]
  
  #most_common_interventional_cardiologist = left_join(
  #  data,
  #  physician_integration_stats,
  #  by = c(
  #    "most_common_interventional_cardiologist_PRF_PHYSN_NPI" = "PRF_PHYSN_NPI")
  #) %>% as.data.table()
  #rename_last(
  #  most_common_interventional_cardiologist,
  #  ncol(physician_integration_stats)-1,
  #  paste("most_common_interventional_cardiologist_",colnames(physician_integration_stats)[2:ncol(physician_integration_stats)],sep="")
  #  )
  #most_common_interventional_cardiologist=most_common_interventional_cardiologist[,-c("most_common_physician_PRF_PHYSN_NPI"
  #                                                ,"most_common_primary_care_physician_PRF_PHYSN_NPI"
  #                                                ,"most_common_cardiologist_PRF_PHYSN_NPI")]
  
  return(list(most_common_physician,
              most_common_primary_care,
              most_common_cardiologist
              #most_common_interventional_cardiologist
             ))
}

melted_physician_integration_stats_2013_stable_angina=add_integration_status_2013(
  data = yearly_calculations_stable_angina,
  physician_integration_stats = physician_integration_stats)

melted_physician_integration_stats_2013_unstable_angina=add_integration_status_2013(
  data = yearly_calculations_unstable_angina,
  physician_integration_stats = physician_integration_stats)









change_stats_summarizer_non_exclusive_only = function(data,melted_physician_integration_stats,specialty){

  require(dtplyr)
  require(lubridate)
  require(tidyverse)

  looking_for=paste("most_common_",specialty,"_PRF_PHYSN_NPI",sep="")
  date_looking_for=paste("most_common_",specialty,"_date",sep="")
  in_facility_prp_looking_for=paste("most_common_",specialty,"_in_facility_non_exclusive_HCPCS_prp",sep="")
  in_all_count_looking_for=paste("most_common_",specialty,"_in_all_non_exclusive_HCPCS_count",sep="")
  in_facility_count_looking_for=paste("most_common_",specialty,"_in_facility_non_exclusive_HCPCS_count",sep="")
  
  
  result=melted_physician_integration_stats%>%
  group_by(DESY_SORT_KEY)%>%
  mutate(n_integration_stat_year_of_diagnosis=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0 &
           !is.na(!!rlang::sym(in_facility_prp_looking_for)) ,
           length(!!rlang::sym(in_facility_prp_looking_for))],
         
         n_integration_stat_2013=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31") &
           !is.na(!!rlang::sym(in_facility_prp_looking_for)) ,
           length(!!rlang::sym(in_facility_prp_looking_for))],
         
         n_integration_stat_year_before_diagnosis=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
           first_diagnosis-!!rlang::sym(date_looking_for)<=365 &
           !is.na(!!rlang::sym(in_facility_prp_looking_for)) ,
           length(!!rlang::sym(in_facility_prp_looking_for))]
         
        )%>%
  
  summarise(prp_in_year_of_diagnosis_05_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0,
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.5 , na.rm=T)]/
         n_integration_stat_year_of_diagnosis,
         
         prp_in_2013_05_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31"),
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.5, na.rm=T)]/
         n_integration_stat_2013,
         
         prp_in_year_before_diagnosis_05_non_exclusive_HCPCS=
            .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
              first_diagnosis-!!rlang::sym(date_looking_for)<=365,
              sum(!!rlang::sym(in_facility_prp_looking_for)>=0.5, na.rm=T)]/
         n_integration_stat_year_before_diagnosis,
         
         prp_in_year_of_diagnosis_03_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0,
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.3, na.rm=T)]/
         n_integration_stat_year_of_diagnosis,
         
         prp_in_2013_03_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31"),
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.3, na.rm=T)]/
         n_integration_stat_2013,
         
         prp_in_year_before_diagnosis_03_non_exclusive_HCPCS=
            .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
              first_diagnosis-!!rlang::sym(date_looking_for)<=365,
              sum(!!rlang::sym(in_facility_prp_looking_for)>=0.3, na.rm=T)]/
         n_integration_stat_year_before_diagnosis,
         
        avg_in_year_of_diagnosis_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0,
           sum(!!rlang::sym(in_facility_count_looking_for), na.rm=T) / sum(!!rlang::sym(in_all_count_looking_for), na.rm=T)],

         
         avg_in_2013_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31"),
           sum(!!rlang::sym(in_facility_count_looking_for), na.rm=T) / sum(!!rlang::sym(in_all_count_looking_for), na.rm=T)],
         
         avg_in_year_before_diagnosis_non_exclusive_HCPCS=
            .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
              first_diagnosis-!!rlang::sym(date_looking_for)<=365,
              sum(!!rlang::sym(in_facility_count_looking_for), na.rm=T) / sum(!!rlang::sym(in_all_count_looking_for), na.rm=T)]

        )%>%
  distinct()%>%
  as.data.table()
  
  #rename last n columns in a dataset
  rename_last = function(data, how_many, new_names) {
    total_cols = ncol(data)
    setnames(data, (total_cols - how_many + 1):(total_cols), new_names)
  }
  
  rename_last(
    result,
    9,
    paste(specialty,colnames(result)[(ncol(result)-8):ncol(result)],sep="_")
    )
  
  data = left_join(
    data,
    result,
    by = "DESY_SORT_KEY"
  ) %>% as.data.table()
  
  return(data)
  
}










change_stats_summarizer_non_exclusive_only_2013 = function(data,melted_physician_integration_stats_2013,specialty){

  require(dtplyr)
  require(lubridate)
  require(tidyverse)

  looking_for=paste("most_common_",specialty,"_2013_PRF_PHYSN_NPI",sep="")
  date_looking_for=paste("most_common_",specialty,"_2013_date",sep="")
  in_facility_prp_looking_for=paste("most_common_",specialty,"_2013_in_facility_non_exclusive_HCPCS_prp",sep="")
  in_all_count_looking_for=paste("most_common_",specialty,"_2013_in_all_non_exclusive_HCPCS_count",sep="")
  in_facility_count_looking_for=paste("most_common_",specialty,"_2013_in_facility_non_exclusive_HCPCS_count",sep="")
  
  
  result=melted_physician_integration_stats_2013%>%
  group_by(DESY_SORT_KEY)%>%
  mutate(n_integration_stat_year_of_diagnosis=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0 &
           !is.na(!!rlang::sym(in_facility_prp_looking_for)) ,
           length(!!rlang::sym(in_facility_prp_looking_for))],
         
         n_integration_stat_2013=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31") &
           !is.na(!!rlang::sym(in_facility_prp_looking_for)) ,
           length(!!rlang::sym(in_facility_prp_looking_for))],
         
         n_integration_stat_year_before_diagnosis=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
           first_diagnosis-!!rlang::sym(date_looking_for)<=365 &
           !is.na(!!rlang::sym(in_facility_prp_looking_for)) ,
           length(!!rlang::sym(in_facility_prp_looking_for))]
         
        )%>%
  
  summarise(prp_in_year_of_diagnosis_05_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0,
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.5 , na.rm=T)]/
         n_integration_stat_year_of_diagnosis,
         
         prp_in_2013_05_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31"),
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.5, na.rm=T)]/
         n_integration_stat_2013,
         
         prp_in_year_before_diagnosis_05_non_exclusive_HCPCS=
            .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
              first_diagnosis-!!rlang::sym(date_looking_for)<=365,
              sum(!!rlang::sym(in_facility_prp_looking_for)>=0.5, na.rm=T)]/
         n_integration_stat_year_before_diagnosis,
         
         prp_in_year_of_diagnosis_03_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0,
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.3, na.rm=T)]/
         n_integration_stat_year_of_diagnosis,
         
         prp_in_2013_03_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31"),
           sum(!!rlang::sym(in_facility_prp_looking_for)>=0.3, na.rm=T)]/
         n_integration_stat_2013,
         
         prp_in_year_before_diagnosis_03_non_exclusive_HCPCS=
            .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
              first_diagnosis-!!rlang::sym(date_looking_for)<=365,
              sum(!!rlang::sym(in_facility_prp_looking_for)>=0.3, na.rm=T)]/
         n_integration_stat_year_before_diagnosis,
         
        avg_in_year_of_diagnosis_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)-first_diagnosis<=365 &
           !!rlang::sym(date_looking_for)-first_diagnosis>=0,
           sum(!!rlang::sym(in_facility_count_looking_for), na.rm=T) / sum(!!rlang::sym(in_all_count_looking_for), na.rm=T)],

         
         avg_in_2013_non_exclusive_HCPCS=
         .[!!rlang::sym(date_looking_for)<as.IDate("2013-12-31"),
           sum(!!rlang::sym(in_facility_count_looking_for), na.rm=T) / sum(!!rlang::sym(in_all_count_looking_for), na.rm=T)],
         
         avg_in_year_before_diagnosis_non_exclusive_HCPCS=
            .[!!rlang::sym(date_looking_for)-first_diagnosis<=0 &
              first_diagnosis-!!rlang::sym(date_looking_for)<=365,
              sum(!!rlang::sym(in_facility_count_looking_for), na.rm=T) / sum(!!rlang::sym(in_all_count_looking_for), na.rm=T)]

        )%>%
  distinct()%>%
  as.data.table()
  
  #rename last n columns in a dataset
  rename_last = function(data, how_many, new_names) {
    total_cols = ncol(data)
    setnames(data, (total_cols - how_many + 1):(total_cols), new_names)
  }
  
  rename_last(
    result,
    9,
    paste(specialty,"2013",colnames(result)[(ncol(result)-8):ncol(result)],sep="_")
    )
  
  data = left_join(
    data,
    result,
    by = "DESY_SORT_KEY"
  ) %>% as.data.table()
  
  return(data)
  
}











yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only(
  yearly_calculations_stable_angina,melted_physician_integration_stats_stable_angina[[1]],"physician")
yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only(
  yearly_calculations_stable_angina,melted_physician_integration_stats_stable_angina[[2]],"primary_care_physician")
yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only(
  yearly_calculations_stable_angina,melted_physician_integration_stats_stable_angina[[3]],"cardiologist")
#yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only(
#  yearly_calculations_stable_angina,melted_physician_integration_stats_stable_angina[[4]],"interventional_cardiologist")

yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only(
  yearly_calculations_unstable_angina,melted_physician_integration_stats_unstable_angina[[1]],"physician")
yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only(
  yearly_calculations_unstable_angina,melted_physician_integration_stats_unstable_angina[[2]],"primary_care_physician")
yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only(
  yearly_calculations_unstable_angina,melted_physician_integration_stats_unstable_angina[[3]],"cardiologist")
#yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only(
#  yearly_calculations_unstable_angina,melted_physician_integration_stats_unstable_angina[[4]],"interventional_cardiologist")



yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only_2013(
  yearly_calculations_stable_angina,melted_physician_integration_stats_2013_stable_angina[[1]],"physician")
yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only_2013(
  yearly_calculations_stable_angina,melted_physician_integration_stats_2013_stable_angina[[2]],"primary_care_physician")
yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only_2013(
  yearly_calculations_stable_angina,melted_physician_integration_stats_2013_stable_angina[[3]],"cardiologist")
#yearly_calculations_stable_angina=change_stats_summarizer_non_exclusive_only_2013(
#  yearly_calculations_stable_angina,melted_physician_integration_stats_2013_stable_angina[[4]],"interventional_cardiologist")

yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only_2013(
  yearly_calculations_unstable_angina,melted_physician_integration_stats_2013_unstable_angina[[1]],"physician")
yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only_2013(
  yearly_calculations_unstable_angina,melted_physician_integration_stats_2013_unstable_angina[[2]],"primary_care_physician")
yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only_2013(
  yearly_calculations_unstable_angina,melted_physician_integration_stats_2013_unstable_angina[[3]],"cardiologist")
#yearly_calculations_unstable_angina=change_stats_summarizer_non_exclusive_only_2013(
#  yearly_calculations_unstable_angina,melted_physician_integration_stats_2013_unstable_angina[[4]],"interventional_cardiologist")



head(yearly_calculations_stable_angina)
head(yearly_calculations_unstable_angina)




write_fst(yearly_calculations_stable_angina,
          "results_apr/yearly_calculations_stable_angina_with_integration.fst") 
write_fst(yearly_calculations_unstable_angina,
          "results_apr/yearly_calculations_unstable_angina_with_integration.fst")
write_fst(physician_integration_stats,
          "results_apr/physician_integration_stats.fst")

#I am also saving the csv to be able to share the results with colleagues later.
write.csv(yearly_calculations_stable_angina,
          "results_apr/yearly_calculations_stable_angina_with_integration.csv") 
write.csv(yearly_calculations_unstable_angina,
          "results_apr/yearly_calculations_unstable_angina_with_integration.csv")


