library(pagedown)
library(here)
library(purrr)
library(dplyr)

load("names.rda")
names <- cd2
rm(cd2)

param_table <- names %>%
  rename(
    region = Region,
    cd_state = Identity,
    state = State,
    district = District
  ) %>% select(cd_state,state,district,full_name)

target <- c("AR01", "TX34", "AL01")
#so for shiny we would want the target to be specific2
selected <- param_table %>% filter(param_table$cd_state %in% target)

render_report <- function(region = selected$region,
                          cd_state = selected$cd_state,
                          full_name = selected$full_name,
                          state = selected$state,
                          district = selected$district) {

  template <- "CD_template_2024.rmd"

  out_file <- paste0(cd_state, "_2023")

  parameters <- list(cd_state = cd_state,
                     full_name=full_name,
                     state=state,
                     district=district)

  pagedown::chrome_print(rmarkdown::render(template, params = parameters,
    envir = new.env(),clean = TRUE, output_dir = ("Output"), output_file = out_file,
  ))

  htmls <- dir(path="Output",  pattern=".html")
  invisible(file.remove(file.path("Output", htmls))) #delete only the html files
  #remove the invisible() command on the line above if you would like to see in the console files are removed

 # invisible(TRUE)
}

#use this function if you just want to run one report
# render_report()


# render_report(cd_state = selected$cd_state, full_name = selected$full_name, state = selected$state)

#use these two if you want to run multiple reports
params_list <- as.list(selected)

pmap(params_list, render_report)

