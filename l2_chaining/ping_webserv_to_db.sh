#!/bin/bash
source /home/savitb/bin/functions

NAME="$(whoami)"
NAME=`echo ${NAME} | sed 's/\.//g'`

green_desc_title "SSH to web server to ping database from there, and displays results here"
WEBSERV_IP=`heat output-show ${NAME} WebsiteURL | sed 's/http:\/\///g' | sed 's/"//g'`
DB_IP=`heat output-show thomas DatabaseIP | sed 's/"//g'`

command_desc "ssh ubuntu@${WEBSERV_IP} ping -c4 ${DB_IP}"
echo
ssh -o StrictHostKeyChecking=no ubuntu@${WEBSERV_IP} ping -c4 ${DB_IP}

