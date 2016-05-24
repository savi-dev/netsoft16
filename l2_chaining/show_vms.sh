#!/bin/bash
source /home/savitb/bin/functions

REGION1=$OS_REGION_NAME
REGION2=CORE

if [ "$REGION1" == "$REGION2" ]; then
    REGION2=EDGE-TR-1
fi

green_desc_title "Listing VMs in region: $REGION1"
command_desc "nova --os-region-name $REGION1 list"
nova --os-region-name $REGION1 list

green_desc_title "Listing VMs in region: $REGION2"
command_desc "nova --os-region-name $REGION2 list"
nova --os-region-name $REGION2 list
