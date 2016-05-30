#!/bin/bash
source /home/savitb/bin/functions

NAME="$(whoami)"
NAME=`echo ${NAME} | sed 's/\.//g'`

green_desc_title "SSH to web server to ping database from there, and displays results here"

OUTPUT=`heat stack-show ${NAME}`
WEBSERV_IP=`echo "$OUTPUT" | grep WebsiteURL -B2 | grep -e "[0-9.]" | cut -d '"' -f 4 | sed 's/http:\/\///g'`
DB_IP=`echo "$OUTPUT" | grep DatabaseIP -B2 | grep -e "[0-9.]" | cut -d '"' -f 4`


#WEBSERV_IP=`heat output-show ${NAME} WebsiteURL | sed 's/http:\/\///g' | sed 's/"//g'`
#DB_IP=`heat output-show ${NAME} DatabaseIP | sed 's/"//g'`

command_desc "ssh ubuntu@${WEBSERV_IP} ping -c4 ${DB_IP}"
echo
ssh -o StrictHostKeyChecking=no ubuntu@${WEBSERV_IP} ping -c4 ${DB_IP}

