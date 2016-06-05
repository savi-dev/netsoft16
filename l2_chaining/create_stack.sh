#!/bin/bash
source /home/savitb/bin/functions
if [ "$#" -ne 2 ]; then
    echo "ERROR: Incorrect number of paramters"
    echo "Usage: ./create_stack.sh <key_name> <edge_region_name>"
    exit 0
fi

POSSIBLE_EDGES="EDGE-WT-1 EDGE-YK-1 EDGE-CT-1 EDGE-VC-1"

NAME="$(whoami)"
NAME=`echo ${NAME} | sed 's/\.//g'`
KEY_NAME=$1

# Where web server is located
REGION1=$2

# Where DB server is located
REGION2=CORE

# Ensure specified region is valid
MATCH=0
for i in $POSSIBLE_EDGES; do
    if [[ "${i}" == "${REGION1}" ]]; then
        MATCH=1
        break;
    fi
done

if [[ $MATCH == 0 ]]; then
    echo "ERROR: Unknown edge name provided"
    exit 0
fi

# Fingerprint of public key
if [[ ! -f ${HOME}/.ssh/id_rsa.pub ]]; then
    echo "ERROR: No public key found. Try running: ssh-keygen"
    exit 0
fi
KEY_FINGERPRINT=`ssh-keygen -lf ${HOME}/.ssh/id_rsa.pub | awk '{print $2}'`

# For region 1:
# If a key with the specified key-name already exists with a different fingerprint, delete it
# If key doesn't exist or it existed with a different fingerprint, upload it
KEY_EXISTS=`nova --os-region-name $REGION1 keypair-list | grep $KEY_NAME | awk '{print $4}'`
if [[ -n ${KEY_EXISTS} && "${KEY_EXISTS}" != "${KEY_FINGERPRINT}" ]]; then
    green_desc_title "A matching key name has been found in ${REGION1} with a different fingerprint, deleting it..."
    command_desc "nova --os-region-name $REGION1 keypair-delete $KEY_NAME"
    nova --os-region-name $REGION1 keypair-delete $KEY_NAME
fi

if [[ "${KEY_EXISTS}" != "${KEY_FINGERPRINT}" ]]; then
    green_desc_title "Uploading new key to ${REGION1} with the name ${KEY_NAME} ..."
    command_desc "nova --os-region-name $REGION1 keypair-add --pub_key $HOME/.ssh/id_rsa.pub $KEY_NAME"
    nova --os-region-name $REGION1 keypair-add --pub_key $HOME/.ssh/id_rsa.pub $KEY_NAME
fi

# Repeat for region 2:
KEY_EXISTS=`nova --os-region-name $REGION2 keypair-list | grep $KEY_NAME | awk '{print $4}'`
if [[ -n ${KEY_EXISTS} && "${KEY_EXISTS}" != "${KEY_FINGERPRINT}" ]]; then
    green_desc_title "A matching key name has been found in ${REGION2} with a different fingerprint, deleting it..."
    command_desc "nova --os-region-name $REGION2 keypair-delete $KEY_NAME"
    nova --os-region-name $REGION2 keypair-delete $KEY_NAME
fi

if [[ "${KEY_EXISTS}" != "${KEY_FINGERPRINT}" ]]; then
    green_desc_title "Uploading new key to ${REGION2} with the name ${KEY_NAME} ..."
    command_desc "nova --os-region-name $REGION2 keypair-add --pub_key $HOME/.ssh/id_rsa.pub $KEY_NAME"
    nova --os-region-name $REGION2 keypair-add --pub_key $HOME/.ssh/id_rsa.pub $KEY_NAME
fi

# Now deploy the Heat stack
green_desc_title "Deploying Heat stack based on template: wordpress_multi_region.yaml"
green_desc_title "Heat stack will be named: ${NAME}"
command_desc "heat stack-create $NAME -f wordpress_multi_region.yaml -P=\"key_name=$KEY_NAME;region1=$REGION1;region2=${REGION2}\""
OUTPUT=`heat stack-create $NAME -f wordpress_multi_region.yaml -P="key_name=$KEY_NAME;region1=$REGION1;region2=$REGION2" 2>&1`
echo

echo "$OUTPUT"

if [[ ! "$OUTPUT" =~ "ERROR" ]]; then
    blue_desc_title "Deployed Heat stack \"$NAME\" with key \"$KEY_NAME\".
The database server will be located on ${REGION2}.
The web server will be located on ${REGION1}."
fi


