#!/bin/bash

REGION1=$OS_REGION_NAME
REGION2=CORE

if [ "$REGION1" == "$REGION2" ]; then
    REGION2=EDGE-TR-1
fi

echo listing vms in region $REGION1
echo
echo nova --os-region-name $REGION1 list 
nova --os-region-name $REGION1 list 

echo listing vms in region $REGION2
echo
echo nova --os-region-name $REGION2 list 
nova --os-region-name $REGION2 list 
