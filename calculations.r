setwd("/work/postresearch/Shared/Projects/Farbod")
library(tidyverse)
library(parallel)
library(data.table)
options(max.print = 2000)
numcores=32
library(fst)
options(width=150)
library(comorbidity)
#install.packages("https://cran.r-project.org/src/contrib/Archive/icd/icd_4.0.9.tar.gz")
library(icd)
library(sqldf)
library(zeallot)
library(reshape)

left_join_pairs_list=function(x,y,by){
    l=length(x)
    result_list=list()

    for (a in 1:l){
        tmp=list(left_join(x[[a]],y[[a]],by=by))
        result_list=append(result_list,tmp)
    }
    return(result_list)
}
#cluster
#month=substr(carrier_data_2020$CLM_THRU_DT[1],5,6)

#read data
carrier_data_2013=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2013.fst")
carrier_data_2014=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2014.fst")
carrier_data_2015=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2015.fst")
carrier_data_2016=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2016.fst")
carrier_data_2017=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2017.fst")
carrier_data_2018=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2018.fst")
carrier_data_2019=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2019.fst")
carrier_data_2020=read.fst("/work/postresearch/Shared/Projects/Data_fst/carrier_data_2020.fst")

inpatient_data_2013 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2013.fst")
inpatient_data_2014 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2014.fst")
colnames(inpatient_data_2013)=colnames(inpatient_data_2014)
inpatient_data_2015 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2015.fst")
inpatient_data_2016 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2016.fst")
inpatient_data_2017 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2017.fst")
inpatient_data_2018 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2018.fst")
inpatient_data_2019 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2019.fst")
inpatient_data_2020 = read.fst("/work/postresearch/Shared/Projects/Data_fst/inpatient_data_2020.fst")

outpatient_data_2013 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2013.fst")
outpatient_data_2014 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2014.fst")
outpatient_data_2015 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2015.fst")
outpatient_data_2016 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2016.fst")
outpatient_data_2017 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2017.fst")
outpatient_data_2018 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2018.fst")
outpatient_data_2019 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2019.fst")
outpatient_data_2020 = read.fst("/work/postresearch/Shared/Projects/Data_fst/outpatient_data_2020.fst")


tot_carrier_finder=function(data){
data%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(year=substr(data$CLM_THRU_DT[1],0,4),
        tot_allowed=sum(LINE_ALOWD_CHRG_AMT))
}

tot_outpatient_finder=function(data){
data%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(year=substr(data$CLM_THRU_DT[1],0,4),
        tot_allowed_outpatient=sum(CLM_TOT_CHRG_AMT))
}

tot_inpatient_finder=function(data){
data%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(year=substr(data$CLM_THRU_DT[1],0,4),
        tot_allowed_inpatient=sum(CLM_TOT_CHRG_AMT))
}

#carrier_data_2013["LINE_ICD_DGNS_CD"]

tot_allowed_carrier=mclapply(X=list(carrier_data_2013,carrier_data_2014,carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020), FUN=tot_carrier_finder, mc.cores=31)
#tot_allowed_carrier

tot_allowed_outpatient=mclapply(X=list(outpatient_data_2013,outpatient_data_2014,outpatient_data_2015,outpatient_data_2016,outpatient_data_2017,outpatient_data_2018,outpatient_data_2019,outpatient_data_2020), FUN=tot_outpatient_finder, mc.cores=31)
#tot_allowed_outpatient

tot_allowed_inpatient=mclapply(X=list(inpatient_data_2013,inpatient_data_2014,inpatient_data_2015,inpatient_data_2016,inpatient_data_2017,inpatient_data_2018,inpatient_data_2019,inpatient_data_2020), FUN=tot_inpatient_finder, mc.cores=31)

#angio_codes_old=c(93451,93452,93453,93454,93455,93456,93457,93458,93459,93460,93461,93462)

patient_calculator_carrier=function(data){

    require(tidyverse)

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

    result=data%>%
        mutate(
            is_catheterization=HCPCS_CD %in% angio_codes,
            is_ecg=HCPCS_CD %in% ecg_codes,
            is_cardiac_ct=HCPCS_CD %in% cardiac_ct_codes,
            is_cardiac_mri=HCPCS_CD %in% cardiac_mri_codes,
            is_stress_test=HCPCS_CD %in% stress_test_codes,
            is_echocardiography=HCPCS_CD %in% echocardiography_codes,
            is_angioplasty=HCPCS_CD %in% angioplasty_codes,
            is_CABG=HCPCS_CD %in% CABG_codes
        )%>%
        group_by(DESY_SORT_KEY)%>%
        summarise(
            year=substr(data$CLM_THRU_DT[1],0,4),
            tot_allowed=sum(LINE_ALOWD_CHRG_AMT),
            stable_angina=sum(ifelse(LINE_ICD_DGNS_VRSN_CD==0, c("I208","I209") %in% LINE_ICD_DGNS_CD, ifelse(LINE_ICD_DGNS_VRSN_CD==9,  "4139" %in% LINE_ICD_DGNS_CD,NA)))>0,
            unstable_angina=sum(ifelse(LINE_ICD_DGNS_VRSN_CD==0, "I200" %in% LINE_ICD_DGNS_CD, ifelse(LINE_ICD_DGNS_VRSN_CD==9,  "4111" %in% LINE_ICD_DGNS_CD,NA)))>0,
            MI=sum(if_else(LINE_ICD_DGNS_VRSN_CD==0, "I21" %in% substr(LINE_ICD_DGNS_CD,0,3), if_else(LINE_ICD_DGNS_VRSN_CD==9, "410" %in% substr(LINE_ICD_DGNS_CD,0,3),NA)))>0,
            cardiac_arrest=sum(if_else(LINE_ICD_DGNS_VRSN_CD==0, "I46" %in% substr(LINE_ICD_DGNS_CD,0,3), if_else(LINE_ICD_DGNS_VRSN_CD==9, "4275" %in% LINE_ICD_DGNS_CD,NA)))>0,
            catheterization= sum(is_catheterization)>0,
            catheterization_count=sum(is_catheterization),
            catheterization_cost=sum(LINE_ALOWD_CHRG_AMT*is_catheterization),
            ecg_count=sum(is_ecg),
            ecg_cost=sum(LINE_ALOWD_CHRG_AMT*is_ecg),
            cardiac_ct_count=sum(is_cardiac_ct),
            cardiac_ct_cost=sum(LINE_ALOWD_CHRG_AMT*is_cardiac_ct),
            cardiac_mri_count=sum(is_cardiac_mri),
            cardiac_mri_cost=sum(LINE_ALOWD_CHRG_AMT*is_cardiac_mri),
            stress_test_count=sum(is_stress_test),
            stress_test_cost=sum(LINE_ALOWD_CHRG_AMT*is_stress_test),
            echocardiography_count=sum(is_echocardiography),
            echocardiography_cost=sum(LINE_ALOWD_CHRG_AMT*is_echocardiography),
            angioplasty_count=sum(is_angioplasty),
            angioplasty_cost=sum(LINE_ALOWD_CHRG_AMT*is_angioplasty),
            CABG_count=sum(is_CABG),
            CABG_cost=sum(LINE_ALOWD_CHRG_AMT*is_CABG)
        )

    return(result)
}

a=patient_calculator_carrier(carrier_data_2016[1:100000,])
data.frame(a[,1:7])
subset(carrier_data_2016,DESY_SORT_KEY==100013401)["LINE_ICD_DGNS_CD"]

patient_carrier_calculations=mclapply(X=list(carrier_data_2013,carrier_data_2014,carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020), FUN=patient_calculator_carrier, mc.cores=31)
save(patient_carrier_calculations, file="patient_carrier_calculations.RData")
load(file="patient_carrier_calculations.RData")
#find comorbidity
#check for icd code types
count_icds=function(data){
    data%>%
    summarise(icd9=sum(LINE_ICD_DGNS_VRSN_CD==9)/n(),icd10=sum(LINE_ICD_DGNS_VRSN_CD==0)/n())
}
count_icds(carrier_data_2013)
count_icds(carrier_data_2014)
count_icds(carrier_data_2015)
count_icds(carrier_data_2016)
count_icds(carrier_data_2017)
count_icds(carrier_data_2018)
count_icds(carrier_data_2019)
count_icds(carrier_data_2020)

#only 2015 has both icd 10 =25% and icd 9=75% , hence i will only include pure patients (only icd 10 or 9), for this I will use a pretty cool method of product and sums

pure_icd_carrier_data_2015=carrier_data_2015[c("DESY_SORT_KEY","LINE_ICD_DGNS_VRSN_CD")]%>%
    group_by(DESY_SORT_KEY)%>%
    summarise(icd_9_pure=ifelse(prod(LINE_ICD_DGNS_VRSN_CD)==0,F,T),icd_10_pure=ifelse(sum(LINE_ICD_DGNS_VRSN_CD)==0,T,F))

pure_icd_carrier_data_2015_data=subset(carrier_data_2015,DESY_SORT_KEY %in% pure_icd_carrier_data_2015[,1][pure_icd_carrier_data_2015[,2]|pure_icd_carrier_data_2015[,3]])

length(unique(pure_icd_carrier_data_2015_data$DESY_SORT_KEY))/length(unique(carrier_data_2015$DESY_SORT_KEY))
#24.2% of the patients are included
pure_icd9_carrier_data_2015=subset(carrier_data_2015,DESY_SORT_KEY %in% subset(pure_icd_carrier_data_2015,icd_9_pure)$DESY_SORT_KEY)
pure_icd10_carrier_data_2015=subset(carrier_data_2015,DESY_SORT_KEY %in% subset(pure_icd_carrier_data_2015,icd_10_pure)$DESY_SORT_KEY)
comorbidity_scores_icd_9=mclapply(X=list(carrier_data_2013,carrier_data_2014,pure_icd9_carrier_data_2015), FUN=comorbidity, id="DESY_SORT_KEY", code="LINE_ICD_DGNS_CD", score="charlson", icd="icd9", assign0 = T, mc.cores=31)
comorbidity_scores_icd_10=mclapply(X=list(pure_icd10_carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020), FUN=comorbidity, id="DESY_SORT_KEY", code="LINE_ICD_DGNS_CD", score="charlson", icd="icd10", assign0 = T, mc.cores=31)
c(carrier_data_2013_comorbidity,carrier_data_2014_comorbidity,pure_icd9_carrier_data_2015_comorbidity) %<-% comorbidity_scores_icd_9
c(pure_icd10_carrier_data_2015_comorbidity,carrier_data_2016_comorbidity,carrier_data_2017_comorbidity,carrier_data_2018_comorbidity,carrier_data_2019_comorbidity,carrier_data_2020_comorbidity) %<-% comorbidity_scores_icd_10
pure_icd9_10_carrier_data_2015_comorbidity=rbind(pure_icd9_carrier_data_2015_comorbidity,pure_icd10_carrier_data_2015_comorbidity)

comorbidity_scores=list(carrier_data_2013_comorbidity,carrier_data_2014_comorbidity,pure_icd9_10_carrier_data_2015_comorbidity,carrier_data_2016_comorbidity,carrier_data_2017_comorbidity,carrier_data_2018_comorbidity,carrier_data_2019_comorbidity,carrier_data_2020_comorbidity)

patient_carrier_calculations=left_join_pairs_list(patient_carrier_calculations,comorbidity_scores,by="DESY_SORT_KEY")

#add inpatient and outpatient costs
patient_carrier_calculations=left_join_pairs_list(patient_carrier_calculations,tot_allowed_outpatient,by="DESY_SORT_KEY")
patient_carrier_calculations=left_join_pairs_list(patient_carrier_calculations,tot_allowed_inpatient,by="DESY_SORT_KEY")
patient_carrier_calculations=lapply(patient_carrier_calculations,subset,select=-c(year,year.y))
patient_carrier_calculations=lapply(patient_carrier_calculations,function(x) {
    colnames(x)[2]="year"
    return(x)})
patient_carrier_calculations=lapply(patient_carrier_calculations,function(x) {
    colnames(x)[3]="tot_allowed_carrier"
    return(x)})

calculate_tot_allowed=function(data){
    data%>%
    mutate(tot_allowed_carrier=replace_na(tot_allowed_carrier,0),tot_allowed_inpatient=replace_na(tot_allowed_inpatient,0),tot_allowed_outpatient=replace_na(tot_allowed_outpatient,0),total_allowed=tot_allowed_carrier+tot_allowed_inpatient+tot_allowed_outpatient)
}

patient_carrier_calculations=lapply(patient_carrier_calculations,calculate_tot_allowed)
save(patient_carrier_calculations, file="patient_carrier_calculations_w_comorbidity.RData")
load(file="patient_carrier_calculations_w_comorbidity.RData")

#find most commonly occuring physician and cardiologist
patient_NPI_counts=function(data){

    require(tidyverse)
    result=data %>%
    group_by(DESY_SORT_KEY,PRF_PHYSN_NPI)%>%
    summarise(n=n())%>%
    arrange(.by_group=T,desc(n))
}

patient_most_common_NPI=function(data){

    require(tidyverse)
    result=data %>%
    group_by(DESY_SORT_KEY,PRF_PHYSN_NPI)%>%
    summarise(n=n())%>%
    arrange(.by_group=T,desc(n))%>%
    slice(1)
}


patient_most_common_NPI_results=mclapply(X=list(carrier_data_2013,carrier_data_2014,carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020), FUN=patient_most_common_NPI, mc.cores=108)
save(patient_most_common_NPI_results, file="patient_most_common_NPI_results.RData")
load(file="patient_most_common_NPI_results.RData")

patient_NPI_counts_results=mclapply(X=list(carrier_data_2013,carrier_data_2014,carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020), FUN=patient_NPI_counts, mc.cores=108)

save(patient_NPI_counts_results, file="patient_NPI_counts_results.RData")
load(file="patient_NPI_counts_results.RData")

#NPI_specialties=left_join(unique(carrier_data_2013$PRF_PHYSN_NPI,carrier_data_2013[c("PRVDR_SPCLTY","PRF_PHYSN_NPI")]),by="PRF_PHYSN_NPI")
PRVDR_SPCLTY
patient_NPI_counts_results
#save(patient_carrier_calculations, file="patient_carrier_calculations.RData")

#import medicare national downloadable file
physician_data=read.csv("https://data.cms.gov/provider-data/sites/default/files/resources/69a75aa9d3dc1aed6b881725cf0ddc12_1639689642/DAC_NationalDownloadableFile.csv")

physician_data["NPI"]=sapply(physician_data["NPI"],as.character)

physician_data_unique=physician_data%>%
    group_by(NPI)%>%
    slice(1)

specialty_data=mclapply(X=list(carrier_data_2013[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2014[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2015[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2016[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2017[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2018[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2019[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")],carrier_data_2020[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")]), FUN=distinct,mc.cores=31)

patient_NPI_counts_results_w_specialty=left_join_pairs_list(patient_NPI_counts_results,specialty_data,by="PRF_PHYSN_NPI")

patient_most_common_NPI_results_w_specialty=left_join_pairs_list(patient_most_common_NPI_results,specialty_data,by="PRF_PHYSN_NPI")

find_most_common_by_specialty=function(data,specialty_code){
    require(tidyverse)

    data%>%
    filter(PRVDR_SPCLTY==specialty_code)%>%
    group_by(DESY_SORT_KEY)%>%
    arrange(.by_group=T,desc(n))%>%
    slice(1)
}

most_common_cardiologists=mclapply(patient_NPI_counts_results_w_specialty,find_most_common_by_specialty,specialty_code="06",mc.cores=31)

#year 2013 did not have a code for interventional cardiology
most_common_interventional_cardiologists=mclapply(patient_NPI_counts_results_w_specialty,find_most_common_by_specialty,specialty_code="C3",mc.cores=31)
#change colnames
patient_most_common_NPI_results=lapply(patient_most_common_NPI_results,function(x){
    colnames(x)=c("DESY_SORT_KEY", "most_common_phys", "n_most_common_phys")
    return (x)})
most_common_interventional_cardiologists=lapply(most_common_interventional_cardiologists,function(x){
    colnames(x)=c("DESY_SORT_KEY", "most_common_interventional_cardiologists", "n_most_common_interventional_cardiologists","interventional_cardiologists_specialty_code")
    return (x)})
most_common_cardiologists=lapply(most_common_cardiologists,function(x){
    colnames(x)=c("DESY_SORT_KEY", "most_common_cardiologists", "n_most_common_cardiologists","cardiologists_specialty_code")
    return (x)})

#add to main patient data
patient_carrier_calculations=left_join_pairs_list(patient_carrier_calculations,patient_most_common_NPI_results,by="DESY_SORT_KEY")
patient_carrier_calculations=left_join_pairs_list(patient_carrier_calculations,most_common_cardiologists,by="DESY_SORT_KEY")
patient_carrier_calculations=left_join_pairs_list(patient_carrier_calculations,most_common_interventional_cardiologists,by="DESY_SORT_KEY")

save(patient_carrier_calculations, file="patient_carrier_calculations_w_comorbidity_w_common_phys.RData")
load(file="patient_carrier_calculations_w_comorbidity_w_common_phys.RData")
colnames(patient_carrier_calculations[[8]])
patient_carrier_calculations_all_years=reduce(patient_carrier_calculations,rbind)
write.csv(patient_carrier_calculations_all_years,"patient_calculations_all_years.csv")

#physician calculations

physician_calculator=function(data,integrated_place_of_service_codes=c("19","22"),all_place_of_service_codes=c("11","19","22"),integration_threshold=0.75,code_list=c("99201","99202","99203","99204","99205","99211","99212","99213","99214","99215")){

    require(tidyverse)
    data=subset(data,HCPCS_CD %in% code_list)

    res=data%>%
    mutate(
        is_facility=LINE_PLACE_OF_SRVC_CD %in% integrated_place_of_service_codes,
        is_all=LINE_PLACE_OF_SRVC_CD %in% all_place_of_service_codes,
    )%>%
    group_by(PRF_PHYSN_NPI)%>%
    summarise(
        in_facility_count=sum(is_facility),
        in_all_count=sum(is_all),
        tot=n(),
    )%>%
    mutate(
        in_facility_prp=in_facility_count/in_all_count,
        in_facility_prp_from_tot=in_facility_count/tot,
        is_integrated=in_facility_prp>=integration_threshold,
        is_integrated_from_tot=in_facility_prp_from_tot>=integration_threshold,
    )

    left_join(res,distinct(data[c("PRF_PHYSN_NPI","PRVDR_SPCLTY")]),by="PRF_PHYSN_NPI")
}

physician_integration_results=mclapply(X=list(carrier_data_2013,carrier_data_2014,carrier_data_2015,carrier_data_2016,carrier_data_2017,carrier_data_2018,carrier_data_2019,carrier_data_2020), FUN=physician_calculator, mc.cores=31)
save(physician_integration_results, file="physician_integration_results.RData")
load(file="physician_integration_results.RData")

physician_integration_results[[1]]=data.frame(physician_integration_results[[1]],year="2013")
physician_integration_results[[2]]=data.frame(physician_integration_results[[2]],year="2014")
physician_integration_results[[3]]=data.frame(physician_integration_results[[3]],year="2015")
physician_integration_results[[4]]=data.frame(physician_integration_results[[4]],year="2016")
physician_integration_results[[5]]=data.frame(physician_integration_results[[5]],year="2017")
physician_integration_results[[6]]=data.frame(physician_integration_results[[6]],year="2018")
physician_integration_results[[7]]=data.frame(physician_integration_results[[7]],year="2019")
physician_integration_results[[8]]=data.frame(physician_integration_results[[8]],year="2020")

physician_integration_results_all_years=reduce(physician_integration_results,rbind)
write.csv(physician_integration_results_all_years,"physician_integration_results_all_years.csv")
physician_integration_results_all_years=read.csv("physician_integration_results_all_years.csv")

percent_integrated_calc=function(data, by_specialty=F){
    require(tidyverse)

    if (by_specialty==T){
        data%>%
        group_by(PRVDR_SPCLTY)%>%
        summarise(percent_integrated=sum(is_integrated,na.rm=T)/n())}
    else {
        data%>%
        summarise(percent_integrated=sum(is_integrated,na.rm=T)/n())
    }

}

percent_integrated=mclapply(X=physician_integration_results, FUN=percent_integrated_calc, mc.cores=31)
percent_integrated=reduce(percent_integrated,cbind)
colnames(percent_integrated)=c("PRVDR_SPCLTY",as.character(2013:2020))

percent_integrated_by_specialty=mclapply(X=physician_integration_results, FUN=percent_integrated_calc, by_specialty=T, mc.cores=31)
percent_integrated_by_specialty=reduce(percent_integrated_by_specialty,full_join,by="PRVDR_SPCLTY")
colnames(percent_integrated_by_specialty)=c("PRVDR_SPCLTY",2013:2020)

write.csv(percent_integrated,"percent_integrated.csv")
write.csv(percent_integrated_by_specialty,"percent_integrated_by_specialty.csv")

percent_integrated=read.csv("percent_integrated.csv")
percent_integrated_by_specialty=read.csv("percent_integrated_by_specialty.csv")

percent_integrated=data.frame(PRVDR_SPCLTY="all",percent_integrated)
percent_integrated_all=rbind(percent_integrated,percent_integrated_by_specialty)[,-2]

#plot integration percentages
melted_integration_by_specialty=melt(percent_integrated_all,id="PRVDR_SPCLTY")

plot_integration_by_specialty=function(data){
    require(ggplot2)
    ggplot(data=data)+geom_path(aes(y=value,x=variable,colour=PRVDR_SPCLTY,group=PRVDR_SPCLTY))+geom_point(aes(y=value,x=variable,colour=PRVDR_SPCLTY,group=PRVDR_SPCLTY))
}

ploted_integration_by_specialty=plot_integration_by_specialty(data=subset(melted_integration_by_specialty,PRVDR_SPCLTY %in% c("all","06","C3")))

library(Cairo)
ggsave(filename = "integration_by_specialty.pdf",
       plot = print(ploted_integration_by_specialty),
       device = cairo_pdf , height = 5, width = 8)

#join patient data with physician integration data and add integration changes
patient_carrier_calculations_all_years=read.csv("patient_calculations_all_years.csv")
physician_integration_results_all_years=read.csv("physician_integration_results_all_years.csv")
physician_integration_results_all_years=physician_integration_results_all_years[,-1]
patient_carrier_calculations_all_years

#find integration status changes
physician_integration_results_all_years_changes=physician_integration_results_all_years%>%
    group_by(PRF_PHYSN_NPI)%>%
    arrange(year)%>%
    mutate(became_Integrated = case_when(is_integrated > lag(is_integrated) ~ TRUE,TRUE ~ FALSE)
)

physician_integration_results_all_years_changes=physician_integration_results_all_years_changes%>%
    arrange(PRF_PHYSN_NPI)

physician_integration_results_all_years_changes=physician_integration_results_all_years_changes%>%
    mutate(year=as.character(year))
data.frame(physician_integration_results_all_years_changes)
write.csv(physician_integration_results_all_years_changes,"physician_integration_results_all_years_changes.csv")

data.frame(physician_integration_results_all_years_changes)

physician_integration_results_all_years_changes_summary=physician_integration_results_all_years_changes%>%
    group_by(year)%>%
    summarise(PRVDR_SPCLTY="all", new_integrations=sum(became_Integrated))

physician_integration_results_all_years_changes_summary_by_specialty=physician_integration_results_all_years_changes%>%
    group_by(year,PRVDR_SPCLTY)%>%
    summarise(new_integrations=sum(became_Integrated))
physician_integration_results_all_years_changes_summary=rbind(physician_integration_results_all_years_changes_summary,physician_integration_results_all_years_changes_summary_by_specialty)

integrated_physicians_plot=ggplot(data=subset(physician_integration_results_all_years_changes_summary,PRVDR_SPCLTY %in% c("all")))+geom_bar(aes(y=new_integrations,x=year,fill=PRVDR_SPCLTY,group=PRVDR_SPCLTY),position="dodge",stat="identity")

integrated_cardiologists_plot=ggplot(data=subset(physician_integration_results_all_years_changes_summary,PRVDR_SPCLTY %in% c("06","C3")))+geom_bar(aes(y=new_integrations,x=year,fill=PRVDR_SPCLTY,group=PRVDR_SPCLTY),position="dodge",stat="identity")

ggsave(filename = "newly_integrated_physicians_plot.pdf",
       plot = print(integrated_physicians_plot),
       device = cairo_pdf , height = 5, width = 5)
ggsave(filename = "newly_integrated_cardiologists_plot.pdf",
       plot = print(integrated_cardiologists_plot),
       device = cairo_pdf , height = 5, width = 5)

#add to the main data
most_common_integration_results_all_years_changes=physician_integration_results_all_years_changes[c("PRF_PHYSN_NPI","year","in_facility_prp","is_integrated","PRVDR_SPCLTY","became_Integrated")]
cardiologist_integration_results_all_years_changes=physician_integration_results_all_years_changes%>%
    filter(PRVDR_SPCLTY=="06")%>%
    summarise(PRF_PHYSN_NPI,year,in_facility_prp,is_integrated,became_Integrated)
data.frame(cardiologist_integration_results_all_years_changes)

interventionist_integration_results_all_years_changes=physician_integration_results_all_years_changes%>%
    filter(PRVDR_SPCLTY=="C3")%>%
    summarise(PRF_PHYSN_NPI,year,in_facility_prp,is_integrated,became_Integrated)


colnames(most_common_integration_results_all_years_changes)=c("most_common_phys","year","most_common_phys_in_facility_prp","most_common_phys_is_integrated","most_common_phys_specialty_code","most_common_phys_became_Integrated")
colnames(cardiologist_integration_results_all_years_changes)=c("most_common_cardiologists","year","most_common_cardiologists_in_facility_prp","most_common_cardiologists_is_integrated","most_common_cardiologists_became_Integrated")
colnames(interventionist_integration_results_all_years_changes)=c("most_common_interventional_cardiologists","year","most_common_interventional_cardiologists_in_facility_prp","most_common_interventional_cardiologists_is_integrated","most_common_interventional_cardiologists_became_Integrated")

patient_calculations_with_integration=left_join(patient_carrier_calculations_all_years,most_common_integration_results_all_years_changes,by=c("year","most_common_phys"))
patient_calculations_with_integration=left_join(patient_calculations_with_integration,cardiologist_integration_results_all_years_changes,by=c("year","most_common_cardiologists"))
patient_calculations_with_integration=left_join(patient_calculations_with_integration,interventionist_integration_results_all_years_changes,by=c("year","most_common_interventional_cardiologists"))

write.csv(patient_calculations_with_integration,"patient_calculations_with_integration.csv")
















