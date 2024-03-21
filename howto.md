# v1.6.0 has been released

## What's new in v1.6.0

We describe in this paragraph the important features added in this
version that might have an impact on your use cases runs.
See the Changelog for the whole list of fixes.

### Support of the DASI ephemeral service

A new ephemeral service is now supported in addition to SBB and NFS.
It is called **DASI** and enables access to a Ganesha NFS server for each DASI root 
(see : https://dasi.readthedocs.io/ for details about DASI). A POSIX filesystem is 
exported on each DASI root. This filesystem is stored itself as a POSIX file whose
name is the name of a dataset built from the namespace listed in the WDF and the DASI
root string declared in DASI configuration file.

#### New fields in the workflow description file

The difference in the WDF comes in the `services` list of dictionaries.
A new type (`DASI`) is now supported, with the following attributes:

```
services:

  - name: testdasi
    type: DASI
    attributes:
      dasiconfig: /path/to/dasi_config_file
      namespace: /path/to/destdir/
      storagesize: 2GiB
      datanodes: 1
      location: partition1
```

**Restrictions**:
- **dasiconfig** (mandatory). It is an absolute path to a readable file. This file is the DASI configuration file, it contains the DASI roots. A Ganesha NFS service is started for each DASI root path. The root path is the mountpoint for the dataset on the compute nodes. As such, it should be declared as an absolute path to a writable directory that should exist on compute nodes.
**Note** that the current version supports only one root path being defined in the DASI configuration file.
**Note** also that since the workflow manager does not run on the appropriate node, it only checks that the string provided in the DASI configuration file corresponds to an absolute pathname.
- **namespace** (mandatory). The namespace is an absolute path to a directory shared by the datanodes
and the login node. The namespace directory should exist and be writable when starting the session that
uses the DASI service. A dataset is stored into this directory for each DASI root path. When the namespace attribute starts with the HESTIA@ prefix the same rules as those described in the section "Support of the Hestia backend for the NFS ephemeral service" apply.
- **storagesize** (mandatory). The storagesize specifies the storage space required for DASI roots. This size will be evenly distributed among the DASI root paths. It can be expressed either with numerical values or with size units.
- **datanodes** (optional - defaults to 1). Since it cannot be greater than 1 in the current version, it is not necessary to use it.
- **location** (optional). It contains the name of the partition the jobs that create and destroy the ephemeral service should be executed on.

## Migration tool

The iosea-migrate command has been added in the wfm-1.6.0 environment.

To use it to migrate data from the POSIX filesystem to an IOSEA dataset, the basic syntax is:
```
iosea-migrate -S <size> <sources> <destination>
```

where:
- **size** is a mandatory parameter, and can be expressed using common units, such as GiB.
- **sources** is one or several files or directories that will be recursively copied to the destination dataset.
- **destination** is the name of the dataset file to create. If prefixed by HESTIA@ it will be created in Hestia, otherwise it will be a regular file on the filesystem.
This file can be used for instance in the namespace field of an NFS service in a workflow description file to be used as dataset source when launching that service.

## Moving from wfm-api 1.4.2 to wfm-api 1.6.0:

Apply what was described below for moving from 1.1.0 to 1.2.0.



# v1.4.0 has been released

## What's new in v1.4.0

We describe in this paragraph the important features added in this
version that might have an impact on your use cases runs.
See the Changelog for the whole list of fixes.

### Support of the Hestia backend for the NFS ephemeral service

The NFS ephemeral service now enables access to a Hestia server in addition to the already
supported Ganesha NFS server. The header and the data objects ids are stored inside a POSIX
file whose name is the name of a dataset listed in the WDF.

#### New namespace format in the workflow description file
The difference in the WDF comes in the services list of dictionaries, for the `NFS` type.
The namespace attribute can now be prefixed by the `HESTIA@` string, denoting that HESTIA special processing should be achieved:

```
services:

  - name: testgbf000
    type: NFS
    attributes:
      namespace: HESTIA@/path/to/destdir/dataset000.txt
      mountpoint: /path/to/mountdir000
      storagesize: 2GiB
      datanodes: 1
      location: partition1
```

#### Restrictions:

* namespace (mandatory) is an absolute path to a file located in a directory shared by the datanodes and the login node.
The namespace base directory should exist and be writable when starting the session that uses the NFS service.
The file name used in the namespace attribute is actually the name of a dataset:
this is the name of the file where the references of the Hestia object are stored.
See below the rules that are applied for the dataset creation / usage.
* mountpoint (mandatory) is an absolute path to a writable directory that should exist
when starting the session that uses the NFS service. This is the dataset mountpoint on the compute nodes. (_no change since previous version_)
*Note*: since the workflow manager does not run on the appropriate node,
the only check it is able to do is that the mountpoint string corresponds to an absolute pathname.
* storagesize (mandatory) can be expressed either with pure numeric values or with size units. (_no change since previous version_)
* datanodes (optional - defaults to 1).
Since it cannot be greater than 1 in the current version, it is not necessary to use it. (_no change since previous version_)
* location (optional) contains the name of the partition the jobs that create and destroy
the ephemeral service should be executed on.
This partition should be the same as the one used to run the workflow steps. (_no change since previous version_)

#### Rules for the namespace attribute

The following rules apply when the namespace attribute starts with  the HESTIA@ prefix.

| Namespace | Charateristics          | Rule |
| --------- | ----------------------- | ----- |
| File      | Non existing            | The dataset will be created after the session is stopped. After ceation, the file will contain 2 object ids: 1 for an internal header, 1 for the actual data stored as a single object |
| File      | Existing that contains 2 lines,1 id on each line | The file will be used: at the beginning of the session to retrieve the data from Hestia with its ids, at the end of the session to store the ids as explained above |
| File      | Existing, empty         | Error |
| File      | Existing, not the expected format | Error |
| Directory | Newest file has the expected format | This file will be used: at the beginning of the session to retrieve the data from Hestia with its ids, at the end of the session to store the ids as explained above |
| Directory | Newest file does not have the expected format | Error |
| Directory | Empty                   | Error |

## Moving from wfm-api 1.2.0 to wfm-api 1.4.0:

Apply what was described below for moving from 1.1.0 to 1.2.0.

# v1.2.0 has been released

## What's new in v1.2.0

We describe in this paragraph the important features added in this
version that might have an impact on your use cases runs.
See the Changelog for the whole list of fixes.

### Support of the NFS ephemeral service

A new ephemeral service is now supported in addition to SBB.
It is called **NFS** and enables access to a Ganesha NFS server, configured to export a POSIX
filesystem. This filesystem is stored itself as a POSIX file whose name is the name of a dataset
listed in the WDF.

#### New fields in the workflow description file

The difference in the WDF comes in the `services` list of dictionaries.
A new type (`NFS`) is now supported, with the following attributes:

```
services:

  - name: testgbf000
    type: NFS
    attributes:
      namespace: /path/to/destdir/dataset000.txt
      mountpoint: /path/to/mountdir000
      storagesize: 2GiB
      datanodes: 1
      location: partition1
```

**Restrictions**:
- **namespace** (mandatory).
It is an absolute path to a file located in a directory shared by the datanodes and
the login node. The `namespace` base directory should exist and be writable when starting
the session that uses the NFS service. The file name used in the `namespace` attribute is actually
the name of a dataset: this is the name of the file where the POSIX filesystem exported by the Ganesha NFS server is stored.
If the file does not exist, a dataset will be created. If the file exists, the corresponding dataset will be accessed.
- **mountpoint** (mandatory).
It is an absolute path to a writable directory that should exist when
starting the session that uses the NFS service.
This is the dataset mountpoint on the compute nodes.
**Note** that since the workflow manager does not run on the appropriate node,
it does not check the previously listed conditions,
it only checks that the provided string corresponds to an absolute pathname.
- **storagesize** (mandatory). It can be expressed either with pure numeric values or with size units.
- **datanodes** (optional - defaults to 1). Since it cannot be greater than 1 in the current version, it is not necessary to use it.
- **location** (optional). It contains the name of the partition the jobs that create and destroy the ephemeral service should be executed on.

### Support of the new "iosea-wf access" command

A new command was introduced: `iosea-wf access`.
It accepts a session name as parameter and outputs the command that should be run to get access to the named session.

This command gives access to the datasets the ephemeral services are configured for
when the command is invoked. This enables for example to interactively check the datasets content.

```
$ iosea-wf access -s session001
Type the following command in order to get access to session session001:
      /usr/bin/srun -J interactive  -N 1 -n 1 --bb "#BB_LUA SBB use_persistent Name=derbeyn-session001-lqcd-sbb1" --pty bash
Then type ^C to exit
```

### Global status output

Typing the command `iosea-wf status` without parameters provides, in a single output,
information about all the sessions, as well as the steps and the services in these sessions.

```
$ iosea-wf status
SESSION    WORKFLOW     STATUS   STEP    STATUS   JOBID SERVICE                      TYPE STATUS    STEP COMMAND
s1         test1        starting s1_a_1  stopped  -     e1                           SBB  stopped   command1
s1         test1        starting s1_a_2  stopped  -     e1                           SBB  stopped   command1
s2         test1        starting s2_a_1  stopped  -     e2                           SBB  stopped   command2
session001 My_Workflow1 starting step_A1 running  3699  derbeyn-session001-lqcd-sbb1 SBB  stagingin sbatch /home_nfs/derbeyn/DM/IO-SEA/WDF/kea3/sbatch_script1.sh
session001 My_Workflow1 starting step_B1 inactive -     derbeyn-session001-lqcd-sbb1 SBB  stagingin sbatch /home_nfs/derbeyn/DM/IO-SEA/WDF/kea3/sbatch_script2.sh
```

## Moving from wfm-api 1.1.0 to wfm-api 1.2.0:

1. Make sure any session started with the previous version of the WFM API has been stopped.
If needed, stop any remaining session.

2. clean the WFM database corresponding to the previous version:
```
$ rm $HOME/.wfm-api.db
```
3. update your PATH in your .bash_profile:
```
$ export PATH=/p/scratch/iosea/wfm/wfm-1.2.0/iosea_wfm_venv/bin:$PATH
```
**Note**: the PATH setting should replace any previous iosea commands aliasing.
This is because the `iosea_wfm_venv` now embeds a `yamllint` command that is
called from the CLI to check your WDF is syntactically correct.

If you used to set aliases for the iosea-wf and wfm-api commands in a
.bash_profile file, remove these settings.

4. Make sure the new iosea commands are the ones you will actually execute:
```
$ type iosea-wf
iosea-wf is aliased to `/p/scratch/iosea/wfm/wfm-1.2.0/iosea_wfm_venv/bin/iosea-wf'
$ type wfm-api
iosea-wf is aliased to `/p/scratch/iosea/wfm/wfm-1.2.0/iosea_wfm_venv/bin/wfm-api'
```
5. Make sure you have access to the yamllint command:
```
$ type yamllint
yamllint is /p/scratch/iosea/wfm/wfm-1.2.0/iosea_wfm_venv/bin/yamllint
```

# v1.1.0 has been released

## What's new in v1.1.0

We describe in this paragraph the important features added in this
version that might have an impact on your use cases runs.

### yamllint introduction

Starting from version 1.1.0, a WDF is expected to be syntactilly correct when
it is submitted for a session start: the CLI now calls `yamllint` on each WDF
before doing anything else, in order to check that the file is syntactically
correct.
So from now on, you might get failures that you were not used to see at the very
beginning of the `iosea-wf start` command:
just fix your WDF before submitting it to the session start.
Alternatively, you can call
```
$ yamllint <your workflow description file>
```
and have every error fixed before actually calling any `iosea-wf start` command.

### Asynchronous stop of the session

Prior to v1.1.0, any `iosea-wf stop session` command used to be executed synchronously.
This means that upon return from this call, we were ensured the WFM database was clean
and the name of the stopped session could be reused without any error.
With the asynchronous stop (that was introduced because of risk of timeouts), we are not
in the same situation anymore. In order to update the session status and to make it
removed from the WFM database, we need to call the command
`iosea-wf status -s <the freshly stopped session>` until that session has disappeared
from the WFM database.

### Parallel runs of the same step

Starting from version 1.1.0, the same step is allowed to be run several times in
parallel inside a single session.
If ever you had scripts that used to check steps status and wait for them to be stopped
before running them once more, you can now remove these checks and run any step as many
times as you want, without taking care.

Of course, if you actually need the same step not to be run several times in parallel,
you can leave your scripts unchanged.

Note that the status command now outputs step "instances" in the following format:
- the step name is prefixed by the owner login and the owning session
- it is suffixed by an "instance id"

Example:
```
[derbey1@deepv ~]$ iosea-wf status -s session110 -T
ID         INSTANCE                                 STATUS          JOBID
1          derbey1-session110-step1_1               stopped         367280
2          derbey1-session110-step1_2               stopped         367281
3          derbey1-session110-step1_3               stopped         367282
4          derbey1-session110-step2_1               stopped         367283
5          derbey1-session110-step2_2               running         367284
6          derbey1-session110-step2_3               running         367285
```

## Moving from wfm-api 1.0.0 to wfm-api 1.1.0:

1. Make sure any session started with the previous version of the WFM API has been stopped:
```
$ iosea-wf status -a
2023-06-09 07:44:17.826 | ERROR    | iosea_wf.utils.utils:output_results:111 - No session found in the WFDB
```
If needed, stop any remaining session

2. clean the WFM database corresponding to the previous version:
```
$ rm $HOME/.wfm-api.db
```
3. set your PATH in your .bash_profile:
```
$ export PATH=/p/scratch/iosea/wfm/wfm-1.1.0/iosea_wfm_venv/bin:$PATH
```
**Note**: the PATH setting should replace any previous iosea commands iliasing.
This is because the `iosea_wfm_venv` now embeds a `yamllint` command that is
called from the CLI to check your WDF is syntactically correct.

If you used to set aliases for the iosea-wf and wfm-api commands in a
.bash_profile file, remove these settings.

4. Make sure the new iosea commands are the ones you will actually execute:
```
$ type iosea-wf
iosea-wf is aliased to `/p/scratch/iosea/wfm/wfm-1.1.0/iosea_wfm_venv/bin/iosea-wf'
$ type wfm-api
iosea-wf is aliased to `/p/scratch/iosea/wfm/wfm-1.1.0/iosea_wfm_venv/bin/wfm-api'
```
5. Make sure you have access to the yamllint command:
```
$ type yamllint
yamllint is /p/scratch/iosea/wfm/wfm-1.1.0/iosea_wfm_venv/bin/yamllint
```
