#To understand the differences between the integrated and non-integrated docs, I want to use Radar plots. I will try to do this in both python and R.
#for R, I will use fmsb package

#we will first install the fmsb package:
#install.packages("fmsb")

#load the package to the R environment
library(fmsb)

# I will use tidyverse packages and data.table to handle the data
library(tidyverse)
library(data.table)
library(dtplyr)
#load the data
stable_angina_primary_care_pure_comparisons=read.csv(file = "/Users/F4RBOD/Documents/Globus_2013_MBP/results_yearly/stable_angina_primary_care_pure_comparisons.csv")

#see what the data looks like


stable_angina_primary_care_pure_comparisons_costs=data.frame(stable_angina_primary_care_pure_comparisons[c(2,3),
                                                                                              .(total_exp,tot_allowed_inpatient,tot_allowed_outpatient,tot_allowed_carrier,
                                                                                                cardiac_arrest,
                                                                                                stroke,
                                                                                                hospitalized_patients,
                                                                                                catheterization,
                                                                                                stress_test,
                                                                                                echocardiography,
                                                                                                CABG)])
max_min_values=data.frame(total_exp=c(200000,20000),
                          tot_allowed_inpatient=c(100000,10000),
                          tot_allowed_outpatient=c(100000,10000),
                          tot_allowed_carrier=c(10000,1000),
                          cardiac_arrest=c(0.03,0),
                          stroke=c(0.1,0),
                          hospitalized_patients=c(1,0),
                          catheterization=c(1,0),
                          stress_test=c(1,0),
                          echocardiography=c(1,0),
                          CABG=c(0.05,0))
stable_angina_primary_care_pure_comparisons_costs=rbind(max_min_values,stable_angina_primary_care_pure_comparisons_costs)

areas = c("#3E8E7E", "#FABB51")
radarchart(data.frame(stable_angina_primary_care_pure_comparisons_costs),
           cglty = 1,       # Grid line type
           cglcol = "gray", # Grid line color
           pcol = 2:4,      # Color for each line
           plwd = 2,        # Width for each line
           plty = 1,        # Line type for each line
           pfcol = areas,   # Color of the areas
           axistype = 2,

)

legend("topright",
       legend = c("Independent","Integrated"),
       bty = "n", pch = 20, col = areas,
       text.col = "grey25", pt.cex = 2))
ggsave(filename = "stable_patients_cardiologist_comparisons.pdf",
       plot = print(
radarchart(data.frame(stable_angina_primary_care_pure_comparisons_costs),
           cglty = 1,       # Grid line type
           cglcol = "gray", # Grid line color
           pcol = 2:4,      # Color for each line
           plwd = 2,        # Width for each line
           plty = 1,        # Line type for each line
           pfcol = areas,   # Color of the areas
           axistype = 2, ),
device = cairo_pdf , height = 8, width = 10)




#cardiologist
stable_angina_cardiologist_pure_comparisons_costs=data.frame(stable_angina_cardiologist_pure_comparisons[c(2,3),
                                                                                              .(total_exp,tot_allowed_inpatient,tot_allowed_outpatient,tot_allowed_carrier,
                                                                                                cardiac_arrest,
                                                                                                stroke,
                                                                                                hospitalized_patients,
                                                                                                catheterization,
                                                                                                stress_test,
                                                                                                echocardiography,
                                                                                                CABG)])
max_min_values=data.frame(total_exp=c(200000,20000),
                          tot_allowed_inpatient=c(100000,10000),
                          tot_allowed_outpatient=c(100000,10000),
                          tot_allowed_carrier=c(10000,1000),
                          cardiac_arrest=c(0.03,0),
                          stroke=c(0.1,0),
                          hospitalized_patients=c(1,0),
                          catheterization=c(1,0),
                          stress_test=c(1,0),
                          echocardiography=c(1,0),
                          CABG=c(0.05,0))
stable_angina_cardiologist_pure_comparisons_costs=rbind(max_min_values,stable_angina_cardiologist_pure_comparisons_costs)

areas = c("#3E8E7E", "#FABB51")
radarchart(data.frame(stable_angina_cardiologist_pure_comparisons_costs),
           cglty = 1,       # Grid line type
           cglcol = "gray", # Grid line color
           pcol = 2:4,      # Color for each line
           plwd = 2,        # Width for each line
           plty = 1,        # Line type for each line
           pfcol = areas,   # Color of the areas
           axistype = 2,

)

legend("topright",
       legend = c("Independent","Integrated"),
       bty = "n", pch = 20, col = areas,
       text.col = "grey25", pt.cex = 2))


#interventionists
stable_angina_interventional_cardiologist_pure_comparisons_costs=data.frame(stable_angina_interventional_cardiologist_pure_comparisons[c(2,3),
                                                                                              .(total_exp,tot_allowed_inpatient,tot_allowed_outpatient,tot_allowed_carrier,
                                                                                                cardiac_arrest,
                                                                                                stroke,
                                                                                                hospitalized_patients,
                                                                                                catheterization,
                                                                                                stress_test,
                                                                                                echocardiography,
                                                                                                CABG)])
max_min_values=data.frame(total_exp=c(200000,20000),
                          tot_allowed_inpatient=c(100000,10000),
                          tot_allowed_outpatient=c(100000,10000),
                          tot_allowed_carrier=c(10000,1000),
                          cardiac_arrest=c(0.03,0),
                          stroke=c(0.1,0),
                          hospitalized_patients=c(1,0),
                          catheterization=c(1,0),
                          stress_test=c(1,0),
                          echocardiography=c(1,0),
                          CABG=c(0.05,0))
stable_angina_interventional_cardiologist_pure_comparisons_costs=rbind(max_min_values,stable_angina_interventional_cardiologist_pure_comparisons_costs)

areas = c("#3E8E7E", "#FABB51")
radarchart(data.frame(stable_angina_interventional_cardiologist_pure_comparisons_costs),
           cglty = 1,       # Grid line type
           cglcol = "gray", # Grid line color
           pcol = 2:4,      # Color for each line
           plwd = 2,        # Width for each line
           plty = 1,        # Line type for each line
           pfcol = areas,   # Color of the areas
           axistype = 2,

)

legend("topright",
       legend = c("Independent","Integrated"),
       bty = "n", pch = 20, col = areas,
       text.col = "grey25", pt.cex = 2))