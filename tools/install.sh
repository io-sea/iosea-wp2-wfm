#!/bin/bash

if [ $# -ne 1 ]; then
    echo "usage: $0 <archive path>"
    exit
fi

INSTALL_DIR=iosea_all
PYTHON_EXEC=python3.8
ARCHIVE_PATH=$1
VENV_NAME="iosea_wfm_venv"

echo "===== INSTALLING IOSEA WFM Python interface ====="
echo "===== Installing from archive ${ARCHIVE_PATH}"

echo "Python executable ${PYTHON_EXEC}"
echo "Sources will be installed in ${VENV_NAME}"

read -r -p "Press any key to continue..." 

$PYTHON_EXEC -m venv $VENV_NAME
source $VENV_NAME/bin/activate
mkdir -p $INSTALL_DIR

tar -xzf $ARCHIVE_PATH -C $INSTALL_DIR
${VENV_NAME}/bin/pip install -U pip --quiet
${VENV_NAME}/bin/pip install -U $INSTALL_DIR/dist/*.whl --find-links $INSTALL_DIR/dist/deps/ --quiet --disable-pip-version-check
[ $? -eq 0 ] && echo "Packages were successfully installed. You can now launch the API by running ${VENV_NAME}/bin/wfm-api. You can now run the workflow manager by running ${VENV_NAME}/bin/iosea-wf." || echo "Installation failed"

echo "Cleaning up install directory"
rm -r $INSTALL_DIR