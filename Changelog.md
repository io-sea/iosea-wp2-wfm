# Change Log

## 1.6.0 (Unreleased)

### Added

BRDM-6650:
  * Add support for DASI single root service.

## 1.4.2 (Released)

### Added

* Fix missing env var for NFS ephemeral services, preventing them from being traced

## 1.4.0 (Released)

### Added

* BRDM-6628:
  * Implement the "/step/progress/job endpoint"
  * Implement the "iosea-wf update progress" command
* BRDM-6723: Add support for heterogenous jobs
* BRDM-6668: Add support for Hestia backend
* BRDM-6587: Implement the "iosea-wf show flavors" command
* BRDM-6586: Implement the "iosea-wf show locations" command
* BRDM-6585: Ask the RM to do a reservation before allocating the service

### Changed

* BRDM-6703: [HESTIA] Fix the way directories are processed in the namespace attribute
* BRDM-6604: [WFM] Some characters must not be allowed in session names
* BRDM-6699: The iosea-wf CLI should return the command execution status

## 1.3.0 (Released)

### Changed

* BRDM-6667: fixes for release 1.2.0
  * fix access command output
  * fix namespace file processing: should not be created if not existing + not empty if used as a datasrc
  * fix session remaining in the "starting" status even if the BB is in the "teardown" state

## 1.2.0 (Released)

### Added

* BRDM-6477: Do not use scontrol show burst if slurm uses Lua scripts
* BRDM-6476: Use the appropriate BB_LUA prefix wherever needed if slurm uses Lua scripts
* BRDM-6059: Use the true slurm state into the step status output
* BRDM-6211: Add a "global" status output to the iosea-wf command
* BRDM-6209: iosea-wf status should not report an error when no session in the DB
* BRDM-6315: iosea-wf status should not report an error when no active step in a session
* BRDM-6216: Add the "iosea-wf access" command for the GBF ephemeral services
* Avoid that datasets are used by different sessions in parallel
* BRDM-6214: define the various methods for NFS Ganesha Ephemeral Service
* BRDM-6212: support attributes added for the NFS service
* BRDM-6213: Add the "iosea-wf access" command for the SBB ehpemeral services
* BRDM-6232: Allow to start a session and to run steps without ephemeral
             services

### Changed

* Overwrite batch temporary files (for BB creation / removal) if they already exist
* Output the service type during "iosea-wf status -A" since we have 2 kinds of ephemeral services
* Output the workflow name during session status.

### Removed

N/A

## 1.1.0 (Released)

### Added

* BRDM-6107: Allow to launch the same step several times in parallel
* BRDM-6120: Log any sbatch error occuring during service start or step run.
* BRDM-6102: Make the jobs associated to step run and service removal start only
             after the service creation job has successfully finished.
* BRDM-6108: Finish the session cleanup when getting its status and it was
             asynchronously stopped

### Changed

* BRDM-6186: Trap exception raised by the yaml parser when wdf format is not correct
             Raise HTTP exception when WDF is syntactically erroneous
* BRDM-6109: Fix steps not sorted in the output of the "iosea-wf status" command

### Removed

N/A

## 1.0.0

* Initial release
