install.packages("packrat")
packrat::init("/work/postresearch/Shared/Projects")
install.packages("languageserver")
library(languageserver)
install.packages("tidyverse",   Ncpus = 32)
library(tidyverse)
ggplot()
a=data.frame(1)
View(a)

library(parallel)
no_cores=32
clust1=makeCluster(no_cores)
clust2=makeCluster(no_cores)
clust3=makeCluster(no_cores)

clusterEvalQ(clust, {
  library(dplyr)
  library(haven)
})

carrier_data_list=list.files(path="/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/")
carrier_data_list=paste("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/",carrier_data_list,sep="")
outpatient_data_list=list.files(path="/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/")
outpatient_data_list=paste("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/",outpatient_data_list,sep="")
inpatient_data_list=list.files(path="/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient//")
inpatient_data_list=paste("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/",inpatient_data_list,sep="")

data_list=append(carrier_data_list,outpatient_data_list)
data_list=append(data_list,inpatient_data_list)

clusterExport(clust,"carrier_data_list")
clusterExport(clust,"read_sas_part")


system.time(parLapplyLB(cl=clust, X =inpatient_data_list, fun = haven::read_sas ,n_max=100000))


#stopCluster(clust)

carrier_data_1=parLapplyLB(cl=clust, X =carrier_data_list, fun = haven::read_sas ,n_max=5000000)
carrier_data_2=parLapplyLB(cl=clust, X =carrier_data_list, fun = haven::read_sas ,n_max=10000000, skip=5000000)
carrier_data_3=parLapplyLB(cl=clust, X =carrier_data_list, fun = haven::read_sas ,n_max=15000000, skip=10000000)
carrier_data_4=parLapplyLB(cl=clust, X =carrier_data_list, fun = haven::read_sas ,n_max=20000000, skip=15000000)
carrier_data_5=parLapplyLB(cl=clust, X =carrier_data_list, fun = haven::read_sas, skip=20000000)

stopCluster(clust)

hello