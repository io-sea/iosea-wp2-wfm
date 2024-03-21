# iosea-wf command

## Pre-requisites

Beforehand, you need to have a system with python>=3.8 and pip (usually shipped together).

> Make sure you have the latest versions of pip and setuptools installed !
> If you're unsure, run `pip install --upgrade pip setuptools`

You need to have invoke installed as well (`pip install invoke`)

## User guide

### Installation

invoke install `iosea-wf`

### Running the command

You can run the production command as follows:

`iosea-wf `

```
Usage: iosea-wf [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  run     Runs a step
  start   Starts a session
  status  Returns object status
  stop    Stops a session
```

*Note*: Only the `start` and part of the `status` sub-commands are currently available.

#### start sub-command

```
Usage: iosea-wf start [OPTIONS]

  Starts a session

Options:
  -w, --workflowfile PATH  Workflow Description file path.  [required]
  -s, --session TEXT       The Session name.  [required]
  --help                   Show this message and exit.
```

#### status sub-command

```
Usage: iosea-wf.py status [OPTIONS]

  Returns object status

Status options: [exactly 1 required and mutually exclusive]
  -a, --allsessions   Get the status for all sessions.
  -s, --session TEXT  Get the status for this session name.
  -A, --allservices   Get the status for all services.
  -S, --service TEXT  Get the status for this service name.

Session options: [all required if --step is set]
  -s, --session TEXT  Get the status for this session name.
  -t, --step TEXT     Get the status for this step.

Other options:
  --help              Show this message and exit.
```

#### run sub-command

```
Usage: iosea-wf.py run [OPTIONS]

  Runs a step

Options:
  -s, --session TEXT  The Session name.  [required]
  -t, --step TEXT     The Step name.  [required]
  --help              Show this message and exit.
```

#### stop sub-command

```
Usage: iosea-wf.py stop [OPTIONS]

  Stops a session

Options:
  -s, --session TEXT  The Session name.  [required]
  --help              Show this message and exit.
```

## Developer guide

### CI/CD

### Installation

invoke install --editable

### Deployment of development environment

In order to be able to run the tests the user must be sudoer.
### Running unit tests

To run the unit tests with coverage, you can use the following target:
`invoke test --coverage --venv`

### Building the packages

To build the packages, you can run the target:

`invoke build --outdir ./dist_packages`

This will output wheels in the `dist_packages` repository.

### Building the documentation
