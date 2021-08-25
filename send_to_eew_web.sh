#!/bin/bash
source ~/.profile
HOST=ftp.environmentalenforcementwatch.org

cd ${EEW_HOME}/Output
ftp-ssl -iv $HOST <<EOF
passive
mput *.pdf
bye
EOF
