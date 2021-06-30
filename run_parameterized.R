library(pagedown)
library(here)
library(purrr)

load("names.rda")
names <- cd2
rm(cd2)

param_table <- names %>%
  rename(
    cd_state = Identity,
    state = State
  ) %>% select(cd_state,state,full_name)

target <- c("TX34", "AL01")
#so for shiny we would want the target to be specific2
selected <- param_table %>% filter(param_table$cd_state %in% target)

render_report <- function(cd_state = selected$cd_state,
                          full_name = selected$full_name,
                          state = selected$state) {

  template <- "CD_template.rmd"

  out_file <- paste0(cd_state, "_2021")

  parameters <- list(cd_state = cd_state,
                     full_name=full_name,
                     state=state)

  pagedown::chrome_print(rmarkdown::render(template, params = parameters,
  envir = new.env(),clean = TRUE, output_dir = ("Output"), output_file = out_file,
  ))

 # invisible(TRUE)
}

#use this function if you just want to run one report
render_report()


render_report(cd_state = selected$cd_state, full_name = selected$full_name, state = selected$state)

#use these two if you want to run multiple reports
params_list <- as.list(selected)

pmap(params_list, render_report)

