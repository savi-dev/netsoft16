#!/bin/bash
source /home/savitb/bin/functions

NAME=`whoami | sed 's/\.//g'`

green_desc_title "Displaying status of Heat stack '${NAME}'"
command_desc "heat stack-show ${NAME}"

OUTPUT=`heat stack-show ${NAME}`
echo "$OUTPUT"

WEBSERV_URL=`echo "$OUTPUT" | grep WebsiteURL -B2 | grep -e "[0-9.]" | cut -d '"' -f 4`
DB_IP=`echo "$OUTPUT" | grep DatabaseIP -B2 | grep -e "[0-9.]" | cut -d '"' -f 4`
FIREWALL_IP=`echo "$OUTPUT" | grep FirewallIP -B2 | grep -e "[0-9.]" | cut -d '"' -f 4`

echo
blue_desc "Summary of stack output:"
blue_desc -n "WordPress URL: "; green_desc "${WEBSERV_URL}"
blue_desc -n "Database Server IP: "; green_desc "${DB_IP}"
blue_desc -n "Firewall Server IP: "; green_desc "${FIREWALL_IP}"

