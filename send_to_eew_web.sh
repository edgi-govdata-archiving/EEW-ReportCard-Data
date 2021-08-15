#!/bin/bash
HOST=ftp.environmentalenforcementwatch.org

cd Output
ftp-ssl -iv $HOST <<EOF
passive
mput *.pdf
bye
EOF
