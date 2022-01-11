#languageserver::run()
#setwd("/work/postresearch/Shared/Projects/Farbod")
packrat::off()
numcores=63

options(max.print = 2000)
options(width=130)

library(tidyverse)
library(parallel)
library(data.table)
setDTthreads(numcores)
library(fst)
library(comorbidity)
library(zeallot)
library(reshape)
library(dtplyr)


#calculations per patient with 1 year limits from diagnoses of stable/unstable angina and MI
#I will do these with data.tables instead of dplyr
choose_columns=function(data_list,columns=c("DESY_SORT_KEY","CLAIM_NO","LINE_NUM","CLM_THRU_DT","LINE_PLACE_OF_SRVC_CD","HCPCS_CD","LINE_ICD_DGNS_VRSN_CD","LINE_ICD_DGNS_CD","LINE_ALOWD_CHRG_AMT","PRF_PHYSN_NPI","PRVDR_SPCLTY","PRVDR_STATE_CD")){

  require(data.table)
  data_list=lapply(data_list, function (data) data [,..columns])
  result=rbindlist(data_list)
  return(result)
}

carrier_data_all_years=choose_columns(list(carrier_data_2013,carrier_data_2014,carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020))
carrier_data_all_years[, date:=as.IDate(as.character(CLM_THRU_DT),"%Y%m%d")][order(date)]
#write_fst(carrier_data_all_years,"carrier_data_all_years.fst")

outpatient_data_all_years= choose_columns(data_list = list(outpatient_data_2013,outpatient_data_2014,outpatient_data_2015,outpatient_data_2016,outpatient_data_2017,outpatient_data_2018,outpatient_data_2019,outpatient_data_2020),columns= c("DESY_SORT_KEY","CLM_THRU_DT","CLM_TOT_CHRG_AMT"))
outpatient_data_all_years[, date:=as.IDate(as.character(CLM_THRU_DT),"%Y%m%d")][order(date)]
#write_fst(outpatient_data_all_years,"outpatient_data_all_years.fst")

inpatient_data_all_years= choose_columns(data_list = list(inpatient_data_2013,inpatient_data_2014,inpatient_data_2015,inpatient_data_2016,inpatient_data_2017,inpatient_data_2018,inpatient_data_2019,inpatient_data_2020),columns= c("DESY_SORT_KEY","CLM_THRU_DT","CLM_TOT_CHRG_AMT","CLM_DRG_CD"))
inpatient_data_all_years[, date:=as.IDate(as.character(CLM_THRU_DT),"%Y%m%d")][order(date)]
#write_fst(inpatient_data_all_years,"inpatient_data_all_years.fst")


carrier_data_all_years=read_fst("carrier_data_all_years.fst", as.data.table = T)
outpatient_data_all_years=read_fst("outpatient_data_all_years.fst", as.data.table = T)
inpatient_data_all_years=read_fst("inpatient_data_all_years.fst", as.data.table = T)


yearly_calculator=function(data,time_frame=365,diagnosis){

  #requirements
  require(data.table)
  require(dtplyr)
  require(tidyverse)
  require(comorbidity)
  require(lubridate)

  #diagnosis codes
  #from https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleId=52850&ver=26 and https://www.aapc.com/codes/cpt-codes-range/93451-93533/10
  angio_codes=c(93451,93452,93453,93454,93455,93456,93457,93458,93459,93460,93461,93462,93463,93464,93503,93505,93530,93531,93532,93533)
  #from https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleid=57326&ver=13&keyword=electrocardiogram&keywordType=starts&areaId=all&docType=NCA,CAL,NCD,MEDCAC,TA,MCD,6,3,5,1,F,P&contractOption=all&sortBy=relevance&bc=1
  ecg_codes=c(93000,93005,93010,93040,93041,93042)
  #from https://scct.org/page/CardiacCTCodes include CTangio
  cardiac_ct_codes=c(75571,75572,75573,75574)
  #from https://cardiacmri.com/tech-guide/cpt-codes-relevant-to-cardiac-mri/
  cardiac_mri_codes=c(75557,75559,75561,75563,75565)
  # from https://medicarepaymentandreimbursement.com/2011/07/cardiovascular-stress-testing-cpt-93015.html and https://www.aapc.com/codes/cpt-codes-range/93000-93050/
  stress_test_codes=c(93015,93016,93017,93018)
  #from https://www.aapc.com/codes/cpt-codes-range/93303-93356/20     includes stress echo
  echocardiography_codes=c(93303,93304,93306,93307,93308,93312,93313,93314,93315,93316,93317,93318,93320,93321,93325,93350,93351,93356,93352,93355,93356)
  #from https://www.aapc.com/codes/cpt-codes-range/92920-92979/ and https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleId=57479#:~:text=CPT%20codes%2092928%2C%2092933%2C%2092929,are%20assigned%20to%20APC%200104.    includes balloon and stent
  angioplasty_codes=c(92920,92921,92924,92925,92928,92929,92933,92934,92937,92938,92941,92943,92944,92973,92974,92975,92978,92979,93571,93572,"C9600","C9601","C9602","C9603","C9604","C9605","C9606","C9607","C9608")
  #from https://www.medaxiom.com/clientuploads/webcast_handouts/Coding_for_CABG-Open_Heart_Procedures.pdf and https://www.aapc.com/codes/cpt-codes-range/33016-33999/10    did not include 33517-33530 since these are used in conjunction with 33533-33548 and not alone, did not include 33542,33545,33548 since these are also in conjunction )aneurismectomy and vsd resection
  CABG_codes=c(33510,33511,33512,33513,33514,33516,33533,33534,33535,33536)
  #from http://www.icd9data.com/2015/Volume1/390-459/430-438/default.htm and https://www.icd10data.com/ICD10CM/Codes/I00-I99/I60-I69/I63-
  stroke_icd_9_codes=c(43301,43311,43321,43331,43381,43391,43401,43411,43491)


  result=data%>%
    mutate(
      is_catheterization=HCPCS_CD %in% angio_codes,
      is_ecg=HCPCS_CD %in% ecg_codes,
      is_cardiac_ct=HCPCS_CD %in% cardiac_ct_codes,
      is_cardiac_mri=HCPCS_CD %in% cardiac_mri_codes,
      is_stress_test=HCPCS_CD %in% stress_test_codes,
      is_echocardiography=HCPCS_CD %in% echocardiography_codes,
      is_angioplasty=HCPCS_CD %in% angioplasty_codes,
      is_CABG=HCPCS_CD %in% CABG_codes,
      is_stable_angina=ifelse(LINE_ICD_DGNS_VRSN_CD==0, LINE_ICD_DGNS_CD %in% c("I208","I209") , ifelse(LINE_ICD_DGNS_VRSN_CD==9, LINE_ICD_DGNS_CD == "4139",NA)),
      is_unstable_angina=ifelse(LINE_ICD_DGNS_VRSN_CD==0, LINE_ICD_DGNS_CD == "I200" , ifelse(LINE_ICD_DGNS_VRSN_CD==9,LINE_ICD_DGNS_CD == "4111",NA)),
      is_MI=if_else(LINE_ICD_DGNS_VRSN_CD==0,substr(LINE_ICD_DGNS_CD,0,3) == "I21", if_else(LINE_ICD_DGNS_VRSN_CD==9, substr(LINE_ICD_DGNS_CD,0,3) == "410" ,NA)),
      is_cardiac_arrest=if_else(LINE_ICD_DGNS_VRSN_CD==0, substr(LINE_ICD_DGNS_CD,0,3)=="I46", if_else(LINE_ICD_DGNS_VRSN_CD==9,LINE_ICD_DGNS_CD == "4275",NA)),
      is_stroke=if_else(LINE_ICD_DGNS_VRSN_CD==0, substr(LINE_ICD_DGNS_CD,0,3)=="I63", if_else(LINE_ICD_DGNS_VRSN_CD==9,LINE_ICD_DGNS_CD %in% stroke_icd_9_codes,NA))
    )%>%
    group_by(DESY_SORT_KEY)%>%
    filter(sum(eval(parse(text=paste("is_",diagnosis,sep = ""))),na.rm=T)==T)%>%
    mutate(first_diagnosis=min(date[eval(parse(text=paste("is_",diagnosis,sep = "")))==T]),na.rm = T)%>%
    filter(date-first_diagnosis>=0 & date-first_diagnosis<time_frame,.preserve = T)%>%
    summarise(
      first_diagnosis=unique(first_diagnosis),
      tot_allowed=sum(na.rm = T,LINE_ALOWD_CHRG_AMT),
      catheterization_count=sum(na.rm = T,is_catheterization),
      catheterization_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_catheterization),
      ecg_count=sum(na.rm = T,is_ecg),
      ecg_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_ecg),
      cardiac_ct_count=sum(na.rm = T,is_cardiac_ct),
      cardiac_ct_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_cardiac_ct),
      cardiac_mri_count=sum(na.rm = T,is_cardiac_mri),
      cardiac_mri_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_cardiac_mri),
      stress_test_count=sum(na.rm = T,is_stress_test),
      stress_test_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_stress_test),
      echocardiography_count=sum(na.rm = T,is_echocardiography),
      echocardiography_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_echocardiography),
      angioplasty_count=sum(na.rm = T,is_angioplasty),
      angioplasty_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_angioplasty),
      CABG_count=sum(na.rm = T,is_CABG),
      CABG_cost=sum(na.rm = T,LINE_ALOWD_CHRG_AMT*is_CABG),
      stable_angina=sum(is_stable_angina,na.rm = T)>0,
      unstable_angina=sum(is_unstable_angina,na.rm = T)>0,
      MI=sum(is_MI,na.rm = T)>0,
      cardiac_arrest=sum(is_cardiac_arrest,na.rm = T)>0,
      stroke=sum(is_stroke,na.rm = T)>0,
      icd_9_pure=ifelse(prod(LINE_ICD_DGNS_VRSN_CD,na.rm = T)==0,F,T),
      icd_10_pure=ifelse(sum(LINE_ICD_DGNS_VRSN_CD,na.rm = T)==0,T,F)
    )%>%
    as.data.table()

  #adding comorbidities
  comorbidity_and_phys_data=
    inner_join(data,result[,c("DESY_SORT_KEY","first_diagnosis","icd_9_pure","icd_10_pure")],by="DESY_SORT_KEY")%>%
      filter(date-first_diagnosis>=0 & date-first_diagnosis<time_frame)%>%as.data.table()


  comorbidity_icd_9=comorbidity_and_phys_data%>%
    subset(icd_9_pure==T)

  if (nrow(comorbidity_icd_9)!=0){
    comorbidity_icd_9=comorbidity(as.data.table(comorbidity_icd_9),id="DESY_SORT_KEY", code="LINE_ICD_DGNS_CD", score="charlson", icd="icd9", assign0 = T)}
  else {
    comorbidity_icd_9=data.table(DESY_SORT_KEY=NA,score=NA,index=NA,wscore=NA,windex=NA)
  }

  comorbidity_icd_10=comorbidity_and_phys_data%>%
    subset(icd_10_pure==T)

  if (nrow(comorbidity_icd_10)!=0){
    comorbidity_icd_10=comorbidity(as.data.table(comorbidity_icd_10),id="DESY_SORT_KEY", code="LINE_ICD_DGNS_CD", score="charlson", icd="icd10", assign0 = T)}
  else {
    comorbidity_icd_10=data.table(DESY_SORT_KEY=NA,score=NA,index=NA,wscore=NA,windex=NA)
  }

  comorbidity_all=rbind(
    comorbidity_icd_9[,c("DESY_SORT_KEY","score","index","wscore","windex")]
    ,comorbidity_icd_10[,c("DESY_SORT_KEY","score","index","wscore","windex")]
  )
  result=left_join(result,comorbidity_all,by="DESY_SORT_KEY",)%>% as.data.table()

  #adding most common physicians
  patient_NPI_count_finder=function(data){
    result=data %>%
      group_by(DESY_SORT_KEY,PRF_PHYSN_NPI)%>%
      summarise(n=n())%>%
      arrange(.by_group=T,desc(n))
  }

  patient_NPI_counts=patient_NPI_count_finder(comorbidity_and_phys_data)

  patient_NPI_counts=left_join(patient_NPI_counts,distinct(data[,.(PRF_PHYSN_NPI,PRVDR_SPCLTY)]),by="PRF_PHYSN_NPI")

  find_most_common=function(data){
    data%>%
      group_by(DESY_SORT_KEY)%>%
      arrange(.by_group=T,desc(n))%>%
      slice(1)%>%
      as.data.table()}
  find_most_common_by_specialty=function(data,specialty_code){
    data%>%
      filter(PRVDR_SPCLTY==specialty_code)%>%
      group_by(DESY_SORT_KEY)%>%
      arrange(.by_group=T,desc(n))%>%
      slice(1)%>%
      as.data.table()}

  most_common_physician=find_most_common(patient_NPI_counts)
  most_common_cardiologists=find_most_common_by_specialty(patient_NPI_counts,specialty_code="06")
  most_common_interventional_cardiologists=find_most_common_by_specialty(patient_NPI_counts,specialty_code="C3")

  most_common_physician=data.frame(most_common_physician)%>%
    rename_with( ~ paste0("most_common_physician_", .x))
  most_common_cardiologists=data.frame(most_common_cardiologists)%>%
    rename_with( ~ paste0("most_common_cardiologist_", .x))
  most_common_interventional_cardiologists=data.frame(most_common_interventional_cardiologists)%>%
    rename_with( ~ paste0("most_common_interventional_cardiologist_", .x))

  result=left_join(result,most_common_physician,by=c("DESY_SORT_KEY"="most_common_physician_DESY_SORT_KEY"))
  result=left_join(result,most_common_cardiologists,by=c("DESY_SORT_KEY"="most_common_cardiologist_DESY_SORT_KEY"))
  result=left_join(result,most_common_interventional_cardiologists,by=c("DESY_SORT_KEY"="most_common_interventional_cardiologist_DESY_SORT_KEY"))

  #calculate and add physician integration data
  #this only uses visits to see if a physician is integrated or not (codde list)
  result=as.data.table(result)
  result[, year_first_diagnosis := lubridate::year(first_diagnosis)]

  physician_integration_finder=function(data,integrated_place_of_service_codes=c("19","22"),all_place_of_service_codes=c("11","19","22"),integration_threshold=0.5,code_list=c("99201","99202","99203","99204","99205","99211","99212","99213","99214","99215")){
    data=subset(data,HCPCS_CD %in% code_list)
    result=data%>%
      mutate(
        is_facility=LINE_PLACE_OF_SRVC_CD %in% integrated_place_of_service_codes,
        is_all=LINE_PLACE_OF_SRVC_CD %in% all_place_of_service_codes,
      )%>%
      group_by(PRF_PHYSN_NPI,year)%>%
      summarise(
        in_facility_count=sum(is_facility,na.rm = T),
        in_all_count=sum(is_all,na.rm = T),
        tot=n(),
      )%>%
      mutate(
        in_facility_prp=in_facility_count/in_all_count,
        in_facility_prp_from_tot=in_facility_count/tot,
        is_integrated=in_facility_prp>=integration_threshold,
        is_integrated_from_tot=in_facility_prp_from_tot>=integration_threshold,
      )
  }

  physician_integration_stats=physician_integration_finder(data, integration_threshold=0.5)
  #add to results

  #rename columns
  rename_last = function(data, how_many, new_names) {
    total_cols=ncol(data)
    setnames(data,(total_cols-how_many+1):(total_cols),new_names)
  }

  result=left_join(result,physician_integration_stats,
                   by=c("most_common_physician_PRF_PHYSN_NPI"="PRF_PHYSN_NPI","year_first_diagnosis"="year"))%>%as.data.table()

  rename_last(result,7,
              c("most_common_physician_in_facility_count"
                ,"most_common_physician_in_all_count"
                ,"most_common_physician_tot"
                ,"most_common_physician_in_facility_prp"
                ,"most_common_physician_in_facility_prp_from_tot"
                ,"most_common_physician_is_integrated"
                ,"most_common_physician_is_integrated_from_tot"))

  result=left_join(result,physician_integration_stats,
                   by=c("most_common_cardiologist_PRF_PHYSN_NPI"="PRF_PHYSN_NPI","year_first_diagnosis"="year"))%>%as.data.table()

  rename_last(result,7,
              c("most_common_cardiologist_in_facility_count"
                ,"most_common_cardiologist_in_all_count"
                ,"most_common_cardiologist_tot"
                ,"most_common_cardiologist_in_facility_prp"
                ,"most_common_cardiologist_in_facility_prp_from_tot"
                ,"most_common_cardiologist_is_integrated"
                ,"most_common_cardiologist_is_integrated_from_tot"))


  result=left_join(result,physician_integration_stats,
                   by=c("most_common_interventional_cardiologist_PRF_PHYSN_NPI"="PRF_PHYSN_NPI","year_first_diagnosis"="year"))%>%as.data.table()

  rename_last(result,7,
              c("most_common_interventional_cardiologist_in_facility_count"
                ,"most_common_interventional_cardiologist_in_all_count"
                ,"most_common_interventional_cardiologist_tot"
                ,"most_common_interventional_cardiologist_in_facility_prp"
                ,"most_common_interventional_cardiologist_in_facility_prp_from_tot"
                ,"most_common_interventional_cardiologist_is_integrated"
                ,"most_common_interventional_cardiologist_is_integrated_from_tot"))

  return(as.data.table(result))
}


yearly_calcualtions_carrier_stable_angina=yearly_calculator(carrier_data_all_years,diagnosis="stable_angina")

yearly_calcualtions_carrier_unstable_angina=yearly_calculator(carrier_data_all_years,diagnosis="unstable_angina")

write_fst(yearly_calcualtions_carrier_stable_angina,"yearly_calcualtions_carrier_stable_angina.fst")
write_fst(yearly_calcualtions_carrier_unstable_angina,"yearly_calcualtions_carrier_unstable_angina.fst")



#adding inpatient and outpatient data (12 month interval included)

yearly_calcualtions_carrier_stable_angina=read_fst("yearly_calcualtions_carrier_stable_angina.fst", as.data.table = T)
yearly_calcualtions_carrier_unstable_angina=read_fst("yearly_calcualtions_carrier_unstable_angina.fst", as.data.table = T)

#adding the data to the yearly calculations with an inner join so I will only have data for stable/unstable angina patients. I will also filter based on date in this part.
outpatient_data_all_years_stable_angina=inner_join(outpatient_data_all_years, yearly_calcualtions_carrier_stable_angina,by="DESY_SORT_KEY")%>%as.data.table()
intpatient_data_all_years_stable_angina=inner_join(inpatient_data_all_years, yearly_calcualtions_carrier_stable_angina,by="DESY_SORT_KEY")%>%as.data.table()

#This will take the outpatient data and retun the total outpatient expenditure. It also asks for the time frame which will be compared to the date first diagnosed.
yearly_tot_outpatient=function(data,time_frame=365){
  data%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_outpatient=sum(CLM_TOT_CHRG_AMT))%>%
    as.data.table()
}
outpatient_tot_yearly=yearly_tot_outpatient(outpatient_data_all_years_stable_angina)

#This will return total expenditure from the inpantient data. It will also return the patients with inpatient claims that had DRG codes to define if they were hospitalized, and the number of hospitalizations with.
yearly_calculator_inpatient=function (data, time_frame=365){
  data%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_inpatient=sum(CLM_TOT_CHRG_AMT),
              number_of_hospitalizations=length(unique(date)[is.na(CLM_DRG_CD)==F]),
    )%>%
    mutate(was_hospitalized=number_of_hospitalizations>0)%>%
    as.data.table()
}
inpatient_tot_yearly=yearly_calculator_inpatient(intpatient_data_all_years_stable_angina)

#adding the outpatient and inpatient results to the carrier results
yearly_calculations_stable_angina=left_join(yearly_calcualtions_carrier_stable_angina, outpatient_tot_yearly, by="DESY_SORT_KEY")%>%as.data.table()
yearly_calculations_stable_angina=left_join(yearly_calculations_stable_angina, inpatient_tot_yearly, by="DESY_SORT_KEY")%>%as.data.table()
#finding the total expenditure in one year
yearly_calculations_stable_angina[,total_exp:=sum(tot_allowed,tot_allowed_outpatient,tot_allowed_inpatient,na.rm = T),by=DESY_SORT_KEY]
yearly_calculations_stable_angina[is.na(number_of_hospitalizations)==T,`:=`(number_of_hospitalizations=0,was_hospitalized=0)]
yearly_calculations_stable_angina[is.na(tot_allowed_outpatient)==T,`:=`(tot_allowed_outpatient=0)]
yearly_calculations_stable_angina[is.na(tot_allowed_inpatient)==T,`:=`(tot_allowed_inpatient=0)]

#adding the data to the yearly calculations with an inner join so I will only have data for stable/unstable angina patients. I will also filter based on date in this part.
outpatient_data_all_years_unstable_angina=inner_join(outpatient_data_all_years, yearly_calcualtions_carrier_unstable_angina,by="DESY_SORT_KEY")%>%as.data.table()
intpatient_data_all_years_unstable_angina=inner_join(inpatient_data_all_years, yearly_calcualtions_carrier_unstable_angina,by="DESY_SORT_KEY")%>%as.data.table()

#This will take the outpatient data and retun the total outpatient expenditure. It also asks for the time frame which will be compared to the date first diagnosed.
yearly_tot_outpatient=function(data,time_frame=365){
  data%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_outpatient=sum(CLM_TOT_CHRG_AMT))%>%
    as.data.table()
}
outpatient_tot_yearly=yearly_tot_outpatient(outpatient_data_all_years_unstable_angina)

#This will return total expenditure from the inpantient data. It will also return the patients with inpatient claims that had DRG codes to define if they were hospitalized, and the number of hospitalizations with.
yearly_calculator_inpatient=function (data, time_frame=365){
  data%>%
    filter(date>=first_diagnosis & date-first_diagnosis<time_frame)%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(tot_allowed_inpatient=sum(CLM_TOT_CHRG_AMT),
              number_of_hospitalizations=length(unique(date)[is.na(CLM_DRG_CD)==F]),
    )%>%
    mutate(was_hospitalized=number_of_hospitalizations>0)%>%
    as.data.table()
}
inpatient_tot_yearly=yearly_calculator_inpatient(intpatient_data_all_years_unstable_angina)

#adding the outpatient and inpatient results to the carrier results
yearly_calculations_unstable_angina=left_join(yearly_calcualtions_carrier_unstable_angina, outpatient_tot_yearly, by="DESY_SORT_KEY")%>%as.data.table()
yearly_calculations_unstable_angina=left_join(yearly_calculations_unstable_angina, inpatient_tot_yearly, by="DESY_SORT_KEY")%>%as.data.table()
#finding the total expenditure in one year
yearly_calculations_unstable_angina[,total_exp:=sum(tot_allowed,tot_allowed_outpatient,tot_allowed_inpatient,na.rm = T),by=DESY_SORT_KEY]
yearly_calculations_unstable_angina[is.na(number_of_hospitalizations)==T,`:=`(number_of_hospitalizations=0,was_hospitalized=0)]
yearly_calculations_unstable_angina[is.na(tot_allowed_outpatient)==T,`:=`(tot_allowed_outpatient=0)]
yearly_calculations_unstable_angina[is.na(tot_allowed_inpatient)==T,`:=`(tot_allowed_inpatient=0)]

yearly_calculations_unstable_angina










#comparisons
#I will compare the patients who were treated by the integrated vs non-integrated physicians, cardiologists and interventionists during the study period. Year 2013 did not divide cardiologists and inteventionists. So, We will not include this year in our comparisons of cardiologists and inteventionists.

intensity_comparator=function(data,grouping_var){
  require(tidyverse)
  result=data%>%
    group_by(eval(parse(text=grouping_var)))%>%
    summarise(
      n=n(),
      total_exp=sum(na.rm=T,total_exp)/n(),
      tot_allowed_inpatient=sum(na.rm=T,tot_allowed_inpatient)/n(),
      tot_allowed_outpatient=sum(na.rm=T,tot_allowed_outpatient)/n(),
      tot_allowed_carrier=sum(na.rm=T,tot_allowed)/n(),
      stable_angina=sum(na.rm=T,stable_angina)/n(),
      unstable_angina=sum(na.rm=T,unstable_angina)/n(),
      MI=sum(na.rm=T,MI)/n(),
      cardiac_arrest=sum(na.rm=T,cardiac_arrest)/n(),
      stroke=sum(na.rm=T,stroke)/n(),
      mean_weighted_charlson=mean(wscore,na.rm=T),
      number_of_hospitalizations=sum(na.rm=T,number_of_hospitalizations)/n(),
      hospitalized_patients=sum(na.rm=T,was_hospitalized)/n()
      ,catheterization=sum(na.rm=T,catheterization_count>0)/n()
      ,catheterization_count=sum(na.rm=T,catheterization_count)/n()
      ,catheterization_cost=sum(na.rm=T,catheterization_cost)/n()
      ,ecg=sum(na.rm=T,ecg_count>0)/n()
      ,ecg_count=sum(na.rm=T,ecg_count)/n()
      ,ecg_cost=sum(na.rm=T,ecg_cost)/n()
      ,cardiac_ct=sum(na.rm=T,cardiac_ct_count>0)/n()
      ,cardiac_ct_count=sum(na.rm=T,cardiac_ct_count)/n()
      ,cardiac_ct_cost=sum(na.rm=T,cardiac_ct_cost)/n()
      ,cardiac_mri=sum(na.rm=T,cardiac_mri_count>0)/n()
      ,cardiac_mri_count=sum(na.rm=T,cardiac_mri_count)/n()
      ,cardiac_mri_cost=sum(na.rm=T,cardiac_mri_cost)/n()
      ,stress_test=sum(na.rm=T,stress_test_count>0)/n()
      ,stress_test_count=sum(na.rm=T,stress_test_count)/n()
      ,stress_test_cost=sum(na.rm=T,stress_test_cost)/n()
      ,echocardiography=sum(na.rm=T,echocardiography_count>0)/n()
      ,echocardiography_count=sum(na.rm=T,echocardiography_count)/n()
      ,echocardiography_cost=sum(na.rm=T,echocardiography_cost)/n()
      ,angioplasty=sum(na.rm=T,angioplasty_count>0)/n()
      ,angioplasty_count=sum(na.rm=T,angioplasty_count)/n()
      ,angioplasty_cost=sum(na.rm=T,angioplasty_cost)/n()
      ,CABG=sum(na.rm=T,CABG_count>0)/n()
      ,CABG_count=sum(na.rm=T,CABG_count)/n()
      ,CABG_cost=sum(na.rm=T,CABG_cost)/n(),
      most_common_physician_is_integrated=sum(na.rm=T,most_common_physician_is_integrated)/n(),
      most_common_cardiologist_is_integrated=sum(na.rm=T,most_common_cardiologist_is_integrated)/n(),
      most_common_interventional_cardiologist_is_integrated=sum(na.rm=T,most_common_interventional_cardiologist_is_integrated)/n(),
    )%>%
    as.data.table()
  setnames(result, "eval(parse(text = grouping_var))", grouping_var)
  return(as.data.table(result))
}

#comparisons for stable angina patients
stable_angina_physician_comparisons=intensity_comparator(yearly_calculations_stable_angina,"most_common_physician_is_integrated")
write.csv(stable_angina_physician_comparisons,"stable_angina_physician_comparisons.csv")

stable_angina_cardiologist_comparisons=intensity_comparator(yearly_calculations_stable_angina[year_first_diagnosis!=2013],"most_common_cardiologist_is_integrated")
write.csv(stable_angina_cardiologist_comparisons,"stable_angina_cardiologist_comparisons.csv")

stable_angina_interventional_cardiologist_comparisons=intensity_comparator(yearly_calculations_stable_angina[year_first_diagnosis!=2013],"most_common_interventional_cardiologist_is_integrated")
write.csv(stable_angina_interventional_cardiologist_comparisons,"stable_angina_interventional_cardiologist_comparisons.csv")


#comparisons for unstable angina patients
unstable_angina_physician_comparisons=intensity_comparator(yearly_calculations_unstable_angina,"most_common_physician_is_integrated")
write.csv(unstable_angina_physician_comparisons,"unstable_angina_physician_comparisons.csv")

unstable_angina_cardiologist_comparisons=intensity_comparator(yearly_calculations_unstable_angina[year_first_diagnosis!=2013],"most_common_cardiologist_is_integrated")
write.csv(unstable_angina_cardiologist_comparisons,"unstable_angina_cardiologist_comparisons.csv")

unstable_angina_interventional_cardiologist_comparisons=intensity_comparator(yearly_calculations_unstable_angina[year_first_diagnosis!=2013],"most_common_interventional_cardiologist_is_integrated")
write.csv(unstable_angina_interventional_cardiologist_comparisons,"unstable_angina_interventional_cardiologist_comparisons.csv")


