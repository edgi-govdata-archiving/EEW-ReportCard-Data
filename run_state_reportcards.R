library(pagedown)
library(here)
library(purrr)
library(dplyr)
library(optparse)

option_list = list(
  make_option(c("-s", "--states"), type="character", default=NULL,
              help="Regular Expression of states", metavar="character")
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

if (is.null(opt$states)){
  print_help(opt_parser)
  stop("A regular expression identifying states to be selected must be supplied.n", call.=FALSE)
}

load("names.rda")
names <- cd2
rm(cd2)

param_table <- names %>%
  rename(
    region = Region,
    cd_state = Identity,
    state = State,
    district = District,
    target_year = Target_year
  ) %>% select(region,cd_state,state,district,target_year,full_name)

render_report <- function(region,
                          full_name,
                          state,
                          district,
                          target_year) {

  template <- "State_template.rmd"

  out_file <- paste0(state, "_2021")

  parameters <- list(full_name=full_name,
                     state=state,
                     district=district,
                     target_year=target_year)

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

#use these two if you want to run multiple reports
params_list <- as.list(param_table %>% filter(param_table$region == "State" & grepl(opt$states,param_table$state)))

pmap(params_list, render_report)

