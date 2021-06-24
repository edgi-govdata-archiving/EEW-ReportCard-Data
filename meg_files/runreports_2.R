#The folder containing all of the .rmds and the custom_current.css need to be in the folder
#defined in list.files(here()), in this example that folder is "pagedown"
#the folder listed in line 9 in output_dir, needs to be an already created folder where you want the
#output .pdfs and .htmls to reside
library(pagedown)
library(here)
load("names.rda")
names <- cd2
rm(cd2)

#filter the names dataframe to the region you want to make a report for
selected <- names %>% filter(names$Identity == "TX"| names$Identity == "OK")
cd_state = as.list(selected$Identity)
full_name = as.list(selected$full_name)
#^need to fix full_name but cd_state is fixed!

for (x in cd_state) {
  #this part that changes the link might not be necessary anyway on the shiny side b/c we haven't figured out the link update situation
  system(paste0("sed \'s/address/", x, "/g\' ", "reportcards/templates_meg/senator_template.Rmd > reportcards/templates_meg/temp.Rmd"))
  pagedown::chrome_print(rmarkdown::render(
    here("reportcards/templates_meg/temp.Rmd"), envir = new.env(),clean = TRUE, output_dir = (here("Outputs")),
    output_file = paste0(x,"_2021")))
  unlink("reportcards/templates_meg/temp.Rmd")
}


#left to do: how to get it to move into the GH pages branch - and discriminate based on file extenstion

