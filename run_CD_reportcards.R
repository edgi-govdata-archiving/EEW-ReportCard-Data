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
  ) %>% select(region,cd_state,state,district,full_name)

render_report <- function(region,
                          cd_state,
                          full_name,
                          state,
                          district) {

  template <- "CD_template.rmd"

  out_file <- paste0(cd_state, "_2021")

  parameters <- list(cd_state = cd_state,
                     full_name=full_name,
                     state=state,
                     district=district)

  tryCatch ({
    message( full_name )
    pagedown::chrome_print(rmarkdown::render(template, params = parameters,
                                             envir = new.env(),clean = TRUE, output_dir = ("Output"), output_file = out_file,
    ))
  }, error=function( cond ) {
    message( "Error condition" )
    message( cond )
  })

 # invisible(TRUE)
}

#use this function if you just want to run one report
# render_report()

# render_report(cd_state = selected$cd_state, full_name = selected$full_name, state = selected$state)

#use these two if you want to run multiple reports
# params_list <- as.list(param_table %>% filter(param_table$state %in% c('AZ')))
params_list <- as.list(param_table %>% filter(param_table$region == "CD" & param_table$state %in% c('AZ')))

pmap(params_list, render_report)

