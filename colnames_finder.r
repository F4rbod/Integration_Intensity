
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line13_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line14_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line15_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line16_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line17_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line18_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line19_4.sas7bdat", n_max=1))
colnames(read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Carrier/car_line20_4.sas7bdat", n_max=1))



write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims13.sas7bdat",n_max=1))),"inp13.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims14.sas7bdat",n_max=1))),"inp14.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims15.sas7bdat",n_max=1))),"inp15.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims16.sas7bdat",n_max=1))),"inp16.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims17.sas7bdat",n_max=1))),"inp17.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims18.sas7bdat",n_max=1))),"inp18.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims19.sas7bdat",n_max=1))),"inp19.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Inpatient/inp_claims20.sas7bdat",n_max=1))),"inp20.txt")

write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims13.sas7bdat",n_max=1))),"out13.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims14.sas7bdat",n_max=1))),"out14.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims15.sas7bdat",n_max=1))),"out15.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims16.sas7bdat",n_max=1))),"out16.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims17.sas7bdat",n_max=1))),"out17.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims18.sas7bdat",n_max=1))),"out18.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims19.sas7bdat",n_max=1))),"out19.txt")
write.table(as.vector(colnames(haven::read_sas("/work/postresearch/Shared/Data_raw/Medicare/Claims/Outpatient/out_claims20.sas7bdat",n_max=1))),"out20.txt")

