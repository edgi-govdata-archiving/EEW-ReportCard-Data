#!/bin/bash

source .profile
cd ${EEW_HOME}

# Rscript run_CD_reportcards.R -s '^[A-I]'
# Rscript run_CD_reportcards.R -s '^[J-R]'
# Rscript run_CD_reportcards.R -s '^[S-Z]'
Rscript run_state_reportcards.R -s '^[A-I]'
Rscript run_state_reportcards.R -s '^[J-R]'
Rscript run_state_reportcards.R -s '^[S-Z]'
