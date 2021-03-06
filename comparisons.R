setwd("/work/postresearch/Shared/Projects/Farbod")
packrat::off()
library(tidyverse)
library(parallel)
library(data.table)
options(max.print = 1000)
numcores=128
library(fst)
options(width=160)
library(comorbidity)
library(icd)
library(sqldf)
library(zeallot)
library(reshape)
library(dtplyr)
setDTthreads(numcores)

patient_calculations_with_integration=read.csv("patient_calculations_with_integration.csv")
physician_integration_results_all_years=read.csv("physician_integration_results_all_years.csv")

percent_integrated_calc=function(data, by_specialty="both",threshold=0.75){
    require(tidyverse)

    if (by_specialty==T){
        data%>%
        group_by(PRVDR_SPCLTY,year)%>%
        summarise(
            !!paste0("percent_integrated_",threshold[1],sep="_"):=sum(in_facility_prp>=threshold[1],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[2],sep="_"):=sum(in_facility_prp>=threshold[2],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[3],sep="_"):=sum(in_facility_prp>=threshold[3],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[4],sep="_"):=sum(in_facility_prp>=threshold[4],na.rm=T)/n()
        )
    }

    if(by_specialty=="both"){
        a=data%>%
        group_by(PRVDR_SPCLTY,year)%>%
        summarise(
            !!paste0("percent_integrated_",threshold[1],sep="_"):=sum(in_facility_prp>=threshold[1],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[2],sep="_"):=sum(in_facility_prp>=threshold[2],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[3],sep="_"):=sum(in_facility_prp>=threshold[3],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[4],sep="_"):=sum(in_facility_prp>=threshold[4],na.rm=T)/n()
        )
        b=data%>%
        group_by(year)%>%
        summarise(PRVDR_SPCLTY="all",
            !!paste0("percent_integrated_",threshold[1],sep="_"):=sum(in_facility_prp>=threshold[1],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[2],sep="_"):=sum(in_facility_prp>=threshold[2],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[3],sep="_"):=sum(in_facility_prp>=threshold[3],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[4],sep="_"):=sum(in_facility_prp>=threshold[4],na.rm=T)/n()
        )

        rbind(b,a)
    }

    else {
        data%>%
        group_by(year)%>%
        summarise(
            !!paste0("percent_integrated_",threshold[1],sep="_"):=sum(in_facility_prp>=threshold[1],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[2],sep="_"):=sum(in_facility_prp>=threshold[2],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[3],sep="_"):=sum(in_facility_prp>=threshold[3],na.rm=T)/n(),
            !!paste0("percent_integrated_",threshold[4],sep="_"):=sum(in_facility_prp>=threshold[4],na.rm=T)/n()
        )
    }
}

percent_integrated=percent_integrated_calc(physician_integration_results_all_years,by_specialty="both",threshold=c(0.25,0.5,0.75,1))

percent_integrated

#plot integration percentages
percent_integrated_all=data.frame()
for(a in 3:6){
    b=melt(percent_integrated[,c(1,2,a)],id=c("year","PRVDR_SPCLTY"))
    percent_integrated_all=rbind(percent_integrated_all,b)
}

percent_integrated_all


plot_integration_by_specialty=function(data){
    require(ggplot2)
    ggplot(data=data)+geom_path(aes(y=value,x=year,colour=PRVDR_SPCLTY,group=PRVDR_SPCLTY))+geom_point(aes(y=value,x=year,colour=PRVDR_SPCLTY,group=PRVDR_SPCLTY))+facet_wrap(vars(variable))
}

ploted_integration_by_specialty=plot_integration_by_specialty(data=subset(percent_integrated_all,PRVDR_SPCLTY %in% c("all","06","C3")))

ploted_integration_by_specialty

library(Cairo)
ggsave(filename = "integration_by_specialty_multi_threshold.pdf",
       plot = print(ploted_integration_by_specialty),
       device = cairo_pdf , height = 20, width = 20)




#comparing patients
patient_calculations_with_integration=read.csv("patient_calculations_with_integration.csv")


intensity_comparator=function(data,grouping_var,subsetting_argument){
    require(tidyverse)
    result=data%>%
    filter(eval(parse(text=subsetting_argument)))%>%
    group_by(year,eval(parse(text=grouping_var)))%>%
    summarise(
        n=n()
        ,catheterization=sum(na.rm=T,catheterization)/n()
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
        ,CABG_cost=sum(na.rm=T,CABG_cost)/n()
        ,mean_weighted_charlson=mean(wscore,na.rm=T)
        ,cardiac_arrest=sum(na.rm=T,cardiac_arrest)/n()
    )
    colnames(result)[2]=grouping_var
    result
}

stable_patients_cardiologist_comparisons=intensity_comparator(patient_calculations_with_integration,
    grouping_var="most_common_cardiologists_is_integrated",
    subsetting_argument="stable_angina==T"
)
View(stable_patients_cardiologist_comparisons)
write.csv(stable_patients_cardiologist_comparisons,"stable_patients_cardiologist_comparisons.csv")
stable_patients_cardiologist_comparisons_nona=stable_patients_cardiologist_comparisons[is.na(stable_patients_cardiologist_comparisons[2])==F,]

unstable_patients_cardiologist_comparisons=intensity_comparator(patient_calculations_with_integration,
    grouping_var="most_common_cardiologists_is_integrated",
    subsetting_argument="unstable_angina==T"
)
write.csv(unstable_patients_cardiologist_comparisons,"unstable_patients_cardiologist_comparisons.csv")
unstable_patients_cardiologist_comparisons_nona=unstable_patients_cardiologist_comparisons[is.na(unstable_patients_cardiologist_comparisons[2])==F,]

mi_patients_cardiologist_comparisons=intensity_comparator(patient_calculations_with_integration,
    grouping_var="most_common_cardiologists_is_integrated",
    subsetting_argument="MI==T"
)
write.csv(mi_patients_cardiologist_comparisons,"mi_patients_cardiologist_comparisons.csv")
mi_patients_cardiologist_comparisons_nona=mi_patients_cardiologist_comparisons[is.na(mi_patients_cardiologist_comparisons[2])==F,]

#create graphs
melted_stable_patients_cardiologist_comparisons_nona=melt(data.frame(stable_patients_cardiologist_comparisons_nona),id=c("year","most_common_cardiologists_is_integrated"))

plot_stable_patients_cardiologist_comparisons=ggplot(data=subset(melted_stable_patients_cardiologist_comparisons_nona,variable %in% c("catheterization","ecg","cardiac_ct","stress_test","echocardiography","angioplasty")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_stable_patients_cardiologist_comparisons_n=ggplot(data=subset(melted_stable_patients_cardiologist_comparisons_nona,variable %in% c("n")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_stable_patients_cardiologist_comparisons_cardiac_arrest=ggplot(data=subset(melted_stable_patients_cardiologist_comparisons_nona,variable %in% c("cardiac_arrest")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_stable_patients_cardiologist_comparisons_charlson=ggplot(data=subset(melted_stable_patients_cardiologist_comparisons_nona,variable %in% c("mean_weighted_charlson")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

ggsave(filename = "stable_patients_cardiologist_comparisons.pdf",
       plot = print(plot_stable_patients_cardiologist_comparisons),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_stable_patients_cardiologist_comparisons_n.pdf",
       plot = print(plot_stable_patients_cardiologist_comparisons_n),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_stable_patients_cardiologist_comparisons_cardiac_arrest.pdf",
       plot = print(plot_stable_patients_cardiologist_comparisons_cardiac_arrest),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_stable_patients_cardiologist_comparisons_charlson.pdf",
       plot = print(plot_stable_patients_cardiologist_comparisons_charlson),
       device = cairo_pdf , height = 5, width = 10)


melted_unstable_patients_cardiologist_comparisons_nona=melt(data.frame(unstable_patients_cardiologist_comparisons_nona),id=c("year","most_common_cardiologists_is_integrated"))

plot_unstable_patients_cardiologist_comparisons=ggplot(data=subset(melted_unstable_patients_cardiologist_comparisons_nona,variable %in% c("catheterization","ecg","cardiac_ct","stress_test","echocardiography","angioplasty")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_unstable_patients_cardiologist_comparisons_n=ggplot(data=subset(melted_unstable_patients_cardiologist_comparisons_nona,variable %in% c("n")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_unstable_patients_cardiologist_comparisons_cardiac_arrest=ggplot(data=subset(melted_unstable_patients_cardiologist_comparisons_nona,variable %in% c("cardiac_arrest")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_unstable_patients_cardiologist_comparisons_charlson=ggplot(data=subset(melted_unstable_patients_cardiologist_comparisons_nona,variable %in% c("mean_weighted_charlson")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))



ggsave(filename = "unstable_patients_cardiologist_comparisons.pdf",
       plot = print(plot_unstable_patients_cardiologist_comparisons),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_unstable_patients_cardiologist_comparisons_n.pdf",
       plot = print(plot_unstable_patients_cardiologist_comparisons_n),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_unstable_patients_cardiologist_comparisons_cardiac_arrest.pdf",
       plot = print(plot_unstable_patients_cardiologist_comparisons_cardiac_arrest),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_unstable_patients_cardiologist_comparisons_charlson.pdf",
       plot = print(plot_unstable_patients_cardiologist_comparisons_charlson),
       device = cairo_pdf , height = 5, width = 10)


melted_mi_patients_cardiologist_comparisons_nona=melt(data.frame(mi_patients_cardiologist_comparisons_nona),id=c("year","most_common_cardiologists_is_integrated"))

plot_mi_patients_cardiologist_comparisons=ggplot(data=subset(melted_mi_patients_cardiologist_comparisons_nona,variable %in% c("catheterization","ecg","cardiac_ct","stress_test","echocardiography","angioplasty")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_mi_patients_cardiologist_comparisons_n=ggplot(data=subset(melted_mi_patients_cardiologist_comparisons_nona,variable %in% c("n")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_mi_patients_cardiologist_comparisons_cardiac_arrest=ggplot(data=subset(melted_mi_patients_cardiologist_comparisons_nona,variable %in% c("cardiac_arrest")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

plot_mi_patients_cardiologist_comparisons_charlson=ggplot(data=subset(melted_mi_patients_cardiologist_comparisons_nona,variable %in% c("mean_weighted_charlson")))+geom_line(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,linetype=most_common_cardiologists_is_integrated))+geom_point(aes(x=year,y=value,group=interaction(most_common_cardiologists_is_integrated,variable),color=variable,shape=most_common_cardiologists_is_integrated))

ggsave(filename = "mi_patients_cardiologist_comparisons.pdf",
       plot = print(plot_mi_patients_cardiologist_comparisons),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_mi_patients_cardiologist_comparisons_n.pdf",
       plot = print(plot_mi_patients_cardiologist_comparisons_n),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_mi_patients_cardiologist_comparisons_cardiac_arrest.pdf",
       plot = print(plot_mi_patients_cardiologist_comparisons_cardiac_arrest),
       device = cairo_pdf , height = 5, width = 10)

ggsave(filename = "plot_mi_patients_cardiologist_comparisons_charlson.pdf",
       plot = print(plot_mi_patients_cardiologist_comparisons_charlson),
       device = cairo_pdf , height = 5, width = 10)








#difference in differences analysis. I found a package called did which uses a recent publication's results for staggered time diff in diff. it is great for our purposes.
#the possible problem is it looks for non time varying covariated and that might be a problem if we want to control for the Charlson scores after integration. for this, I will do A diff in diff for the Charlson. If there was no difference there, then we should be fine with using pre integration Charlson. This is what the package uses.
#the package tutorials:
#https://cran.r-project.org/web/packages/did/did.pdf
#https://cran.r-project.org/web/packages/did/
#https://cran.r-project.org/web/packages/did/vignettes/pre-testing.html
#https://cran.r-project.org/web/packages/did/vignettes/multi-period-did.html
#https://cran.r-project.org/web/packages/did/vignettes/did-basics.html
#the article: https://www.sciencedirect.com/science/article/pii/S0304407620303948?via%3Dihub

#install.packages("did")
library(did)
#prepare the data for diff in diff
#I should first find when first each physician became integrated. I should also check for physicians who did not go back to private practice after getting integrated.

#import the physician data
physician_integration_results_all_years_changes=read.csv("physician_integration_results_all_years_changes.csv")
patient_calculations_with_integration=read.csv("patient_calculations_with_integration.csv")

write_fst(physician_integration_results_all_years_changes,"physician_integration_results_all_years_changes.fst")
write_fst(patient_calculations_with_integration,"patient_calculations_with_integration.fst")

did_preparator=function(data){
    require(tidyverse)
    #select only staggered
    data%>%
    group_by(PRF_PHYSN_NPI)%>%
    arrange(year)%>%
    mutate(
        never_gone_back=prod(is_integrated-lag(is_integrated)+1,na.rm=T)!=0,
        first_integrated_on=ifelse(sum(became_Integrated,na.rm=T)==1,year[which.max(became_Integrated)],0)
    )
}

physician_integration_results_all_years_changes_did=did_preparator(physician_integration_results_all_years_changes)
#max(a$never_gone_back,na.rm=T)
#a$never_gone_back
write_fst(physician_integration_results_all_years_changes_did,"physician_integration_results_all_years_changes_did.fst")
physician_integration_results_all_years_changes_did=read_fst("physician_integration_results_all_years_changes_did.fst")

physician_carrier_calculations_all_years_stable_angina=read_fst("physician_carrier_calculations_all_years_stable_angina.fst")

physician_integration_results_all_years_changes_did$year=as.character(physician_integration_results_all_years_changes_did$year)

did_data_all_physicians_stable_angina=inner_join(physician_carrier_calculations_all_years,physician_integration_results_all_years_changes_did,by=c("year","PRF_PHYSN_NPI"))

did_data_all_physicians_stable_angina$year=as.numeric(did_data_all_physicians_stable_angina$year)
did_data_all_physicians_stable_angina$PRF_PHYSN_NPI=as.numeric(did_data_all_physicians_stable_angina$PRF_PHYSN_NPI)
write_fst(did_data_all_physicians_stable_angina,"did_data_all_physicians_stable_angina.fst")

did_data_all_physicians_stable_angina=subset(did_data_all_physicians_stable_angina,never_gone_back==T)
did_data_cardiologists_stable_angina=subset(did_data_all_physicians_stable_angina,PRVDR_SPCLTY=="06")
did_data_cardiologists_stable_angina=subset(did_data_cardiologists_stable_angina,n_unique_patient>=10)

did_data_all_physicians_stable_angina_10=subset(did_data_all_physicians_stable_angina,n_unique_patient>=10)


data.frame(subset(did_data_cardiologists_stable_angina,became_Integrated))




did_stable_angine=att_gt(
    yname="prp_with_cardiac_arrest",
    tname="year",
    idname="PRF_PHYSN_NPI",
    gname="first_integrated_on",
    #xformla=~prp_with_unstable_angina+prp_with_MI+prp_with_cardiac_arrest,
    data=did_data_cardiologists_stable_angina,
    control_group="nevertreated",
    anticipation=0,
    print_details=T,
    allow_unbalanced_panel = T
)

did_stable_angine
ggdid(did_stable_angine)

agg.es=aggte(did_stable_angine, type = "dynamic",na.rm=T)
ggdid(agg.es)






#descriptive comparisons before and after integration
#here I will find all of the newly integrated physicians, pool their before and after stats together, and try to find some descriptive stats. I will need to adjust for inflation later.

#read data
patient_calculations_with_integration=read_fst("patient_calculations_with_integration.fst", as.data.table = T)

physician_integration_results_all_years_changes_did=read_fst("physician_integration_results_all_years_changes_did.fst", as.data.table = T)

physician_carrier_calculations_all_years_stable_angina=read_fst("physician_carrier_calculations_all_years_stable_angina.fst", as.data.table = T)


newly_integrated_physicians=physician_integration_results_all_years_changes_did[first_integrated_on!=0]
newly_integrated_physicians[,year:=as.character(year)]
newly_integrated_cardiologists_stable_angina=newly_integrated_physicians[physician_carrier_calculations_all_years_stable_angina, on = .(PRF_PHYSN_NPI,year),nomatch=NULL][PRVDR_SPCLTY=="06"]

before_after_on_stable_angina_summaries=newly_integrated_cardiologists_stable_angina[,relative_to_integration:=ifelse(year<first_integrated_on,"Before", ifelse(year==first_integrated_on,"On","After"))][,
        .(
            n=.N,
            n_unique_patient=sum(na.rm=T,n_unique_patient)/.N,
            sum_tot_allowed=sum(na.rm=T,tot_allowed)/.N,
            catheterization=sum(na.rm=T,catheterization_count>0)/.N,
            catheterization_count=sum(na.rm=T,catheterization_count)/.N,
            catheterization_cost=sum(na.rm=T,catheterization_cost)/.N,
            ecg=sum(na.rm=T,ecg_count>0)/.N,
            ecg_count=sum(na.rm=T,ecg_count)/.N,
            ecg_cost=sum(na.rm=T,ecg_cost)/.N,
            cardiac_ct=sum(na.rm=T,cardiac_ct_count>0)/.N,
            cardiac_ct_count=sum(na.rm=T,cardiac_ct_count)/.N,
            cardiac_ct_cost=sum(na.rm=T,cardiac_ct_cost)/.N,
            cardiac_mri=sum(na.rm=T,cardiac_mri_count>0)/.N,
            cardiac_mri_count=sum(na.rm=T,cardiac_mri_count)/.N,
            cardiac_mri_cost=sum(na.rm=T,cardiac_mri_cost)/.N,
            stress_test=sum(na.rm=T,stress_test_count>0)/.N,
            stress_test_count=sum(na.rm=T,stress_test_count)/.N,
            stress_test_cost=sum(na.rm=T,stress_test_cost)/.N,
            echocardiography=sum(na.rm=T,echocardiography_count>0)/.N,
            echocardiography_count=sum(na.rm=T,echocardiography_count)/.N,
            echocardiography_cost=sum(na.rm=T,echocardiography_cost)/.N,
            angioplasty=sum(na.rm=T,angioplasty_count>0)/.N,
            angioplasty_count=sum(na.rm=T,angioplasty_count)/.N,
            angioplasty_cost=sum(na.rm=T,angioplasty_cost)/.N,
            CABG=sum(na.rm=T,CABG_count>0)/.N,
            CABG_count=sum(na.rm=T,CABG_count)/.N,
            CABG_cost=sum(na.rm=T,CABG_cost)/.N,
            cardiac_arrest=sum(na.rm=T,prp_with_cardiac_arrest)/.N,
            unstable_angina=sum(na.rm=T,prp_with_unstable_angina)/.N,
            MI=sum(na.rm=T,prp_with_MI)/.N
        )
    ,keyby=relative_to_integration]
write.csv(before_after_on_stable_angina_summaries,"before_after_on_stable_angina_summaries.csv")

per_year_stable_angina_summaries=newly_integrated_cardiologists_stable_angina[,year_relative_to_integration:=as.numeric(year)-as.numeric(first_integrated_on)][,
        .(
            n=.N,
            n_unique_patient=sum(na.rm=T,n_unique_patient)/.N,
            sum_tot_allowed=sum(na.rm=T,tot_allowed)/.N,
            catheterization=sum(na.rm=T,catheterization_count>0)/.N,
            catheterization_count=sum(na.rm=T,catheterization_count)/.N,
            catheterization_cost=sum(na.rm=T,catheterization_cost)/.N,
            ecg=sum(na.rm=T,ecg_count>0)/.N,
            ecg_count=sum(na.rm=T,ecg_count)/.N,
            ecg_cost=sum(na.rm=T,ecg_cost)/.N,
            cardiac_ct=sum(na.rm=T,cardiac_ct_count>0)/.N,
            cardiac_ct_count=sum(na.rm=T,cardiac_ct_count)/.N,
            cardiac_ct_cost=sum(na.rm=T,cardiac_ct_cost)/.N,
            cardiac_mri=sum(na.rm=T,cardiac_mri_count>0)/.N,
            cardiac_mri_count=sum(na.rm=T,cardiac_mri_count)/.N,
            cardiac_mri_cost=sum(na.rm=T,cardiac_mri_cost)/.N,
            stress_test=sum(na.rm=T,stress_test_count>0)/.N,
            stress_test_count=sum(na.rm=T,stress_test_count)/.N,
            stress_test_cost=sum(na.rm=T,stress_test_cost)/.N,
            echocardiography=sum(na.rm=T,echocardiography_count>0)/.N,
            echocardiography_count=sum(na.rm=T,echocardiography_count)/.N,
            echocardiography_cost=sum(na.rm=T,echocardiography_cost)/.N,
            angioplasty=sum(na.rm=T,angioplasty_count>0)/.N,
            angioplasty_count=sum(na.rm=T,angioplasty_count)/.N,
            angioplasty_cost=sum(na.rm=T,angioplasty_cost)/.N,
            CABG=sum(na.rm=T,CABG_count>0)/.N,
            CABG_count=sum(na.rm=T,CABG_count)/.N,
            CABG_cost=sum(na.rm=T,CABG_cost)/.N,
            cardiac_arrest=sum(na.rm=T,prp_with_cardiac_arrest)/.N,
            unstable_angina=sum(na.rm=T,prp_with_unstable_angina)/.N,
            MI=sum(na.rm=T,prp_with_MI)/.N
        )
    ,keyby=year_relative_to_integration]

write.csv(per_year_stable_angina_summaries,"per_year_stable_angina_summaries.csv")










#yearly comparisons
#read data
patient_calculations_with_integration=read_fst("patient_calculations_with_integration.fst", as.data.table = T)
physician_integration_results_all_years_changes_did=read_fst("physician_integration_results_all_years_changes_did.fst", as.data.table = T)
yearly_calculations_stable_angina
yearly_calculations_unstable_angina

yearly_calculations_stable_angina[patient_calculations_with_integration]

colnames(patient_calculations_with_integration)
patient_calculations_with_integration[,c(2,3,26:67)]