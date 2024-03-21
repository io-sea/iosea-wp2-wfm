
## Installing the API:

This part should be done only once for each new released version.

### Copy the iosea_all.tar.gz file on deep:

on nwadmin:
```
$ cd tmp
$ # registry.sf.bds.atos.net IP is 192.168.6.7
$ wget http://registry.sf.bds.atos.net/artifactory/brdm-pypi-snapshot/iosea-wp2-wfm/iosea_all.tar.gz
$ scp -i <*deep_private_key*> iosea_all.tar.gz <*deep_login*>@deep.fz-juelich.de:/p/scratch/iosea/wfm/wfm-<release>
```

### Copy paste the file tools/install.sh on deep (from the iosea-wp2-wfm repository):

```
$ cd /p/scratch/iosea/wfm/wfm-<release>
$ echo > install.sh <<EOF
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
EOF
$
$ ./install.sh ./iosea_all.tar.gz
```

This will create a venv and install wmf API and CLI inside it.

## Updating the API:

Updating the API is only a matter of removing the directory
``/p/scratch/iosea/wfm/wfm-<release>/iosea_wfm_venv/``
and re-running the ``install.sh`` script.


