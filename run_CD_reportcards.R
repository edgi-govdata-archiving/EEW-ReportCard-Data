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
    district = District
  ) %>% select(region,cd_state,state,district,full_name)

render_report <- function(region,
                          cd_state,
                          full_name,
                          state,
                          district) {

  template <- "CD_template_2022.rmd"

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
params_list <- as.list(param_table %>% filter(param_table$region == "CD" & grepl(opt$states,param_table$state)))

pmap(params_list, render_report)

