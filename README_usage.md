
## Private installation on deep:

After product installation step, the Workflow Manager API and the CLI have been installed
in ``/p/scratch/iosea/wfm/wfm-<release>/iosea_wfm_venv/`` venv on deep cluster.

Each user should add the following aliases to his .bash_profile file:
```
alias wfm-api=/p/scratch/iosea/wfm/wfm-<release>/iosea_wfm_venv/bin/wfm-api
alias iosea-wf=/p/scratch/iosea/wfm/wfm-<release>/iosea_wfm_venv/bin/iosea-wf
```

## Details on settings usage:

### WFM API configuration settings:

1. default settings:
```
   server:
     host: 0.0.0.0
     port: <uid>

   logging:
     level: INFO
     path: /tmp/<username>-wfm-api.log
     stderr: false

   database:
     enabled: true
     name: <user_home>/.wfm-api.db"
```

2. user defined settings: any configuration file:
```
   wfm-api --settings <configuration file>
```


### CLI configuration file:

1. default settings:
```
   server:
     host: 0.0.0.0
     port: <uid>

   logging:
     level: WARNING
     path: /tmp/<username>-iosea-wf.log
     stderr: true
```

2. user defined settings:
   1. Custom user configuration file used by default if present:
      > $HOME/.iosea_wf_settings.yml

   2. any configuration file:
```
      wfm-api --settings <configuration file>
```



## Structure of a workflow description file:

The workflow description file is a yaml file. As such it should fulfill the conditions of any yaml file,
i.e. at least be syntactically correct.
Thus we advise you to use yamllint (a linter for yaml files) in order to do a first level of syntax analysis on
your workflow description file before you use it as an input to the iosea-wf command.

```
workflow:   # described by a dictionary
  name: <workflow name>

services:                         # described by a list of dictionaries
  - name: <string>
    type: <string>
    attributes:                   # described by a dictionary
      targets: <string>
      flavor: <string>
      datanodes: <integer>        # Optional, defaults to 1
      location: <string>          # Optional, defaults to default partition
    datamovers:                   # described by a list of dictionaries - Optional
      - name: <string>
        trigger: <string>
        target: <string>
        operation: <string>
        elements:                 # described by a list of strings
          - <string>
           ...
          - <string>
      ...
      - name: <string>
        trigger: <string>
        target: <string>
        operation: <string>
        elements:                 # described by a list of strings
          - <string>
           ...
          - <string>
  ...
  - name: <string>
    type: <string>
    attributes:                   # described by a dictionary
      targets: <string>
      flavor: <string>
      datanodes: <integer>        # Optional, defaults to 1
      location: <string>          # Optional, defaults to default partition
    datamovers:                   # described by a list of dictionaries - Optional
      - name: <string>
        trigger: <string>
        target: <string>
        operation: <string>
        elements:                 # described by a list of strings
          - <string>
           ...
          - <string>
      ...
      - name: <string>
        trigger: <string>
        target: <string>
        operation: <string>
        elements:                 # described by a list of strings
          - <string>
           ...
          - <string>

steps:   # described by a list of dictionaries
  - name: <string>
    location: <string>            # Optional
    command: <string>
    services:                     # described by a list of dictionaries
      - name: <string>
        datamovers:               # described by a list of strings - Optional
          - <string>
          ...
          - <string>
      ...
      - name: <string>
        datamovers:               # described by a list of strings - Optional
          - <string>
          ...
          - <string>
```


## Day-to-day usage:

1. Launch the wfm-api (might be launched in the background)
```
   wfm-api
```

2. Use ``iosea-wf --help`` to get help about the CLI.

The usual order is:
1. write a workflow description file <path_to_your_wdf>

2. start a session:
```
   $ iosea-wf start -w <path_to_your_wdf> -s <session_name> { --define <variable>=<value> }*
```

3. check whether the session status is active:
```
   $ iosea-wf status -s <session_name>
```

4. run the steps one by one:
```
   $ iosea-wf run -s <session_name> -t <step_name> { --define <variable>=<value> }
```

5. if needed get you session steps status:
```
   $ iosea-wf status -s <session_name> -T
```

6. when you're done with the steps, stop the session (all session steps need
   to be in the STOPPED state to be able to do that)
```
   $ iosea-wf stop -s <session_name>
```

7. if you need to forcingly stop a session:
```
   $ iosea-wf stop -f -s <session_name>
```

## Using variables:

Variables can be used inside the workflow description file (in a future release,
variables will be supported too inside the sbatch scripts called by the WDF).
There are  2 kinds of variables:
- session level variables
- step level variables

The syntax to reference a variable in a WDF is: {{ <*variable name*> }}

### Session-level variables:

These variables can be present anywhere in the workflow description file.
They are instanciated when starting a session, if the user defines their value
on the command line through the "**--define**" option of the "**iosea-wf start**"
command (see syntax above).

#### Session-level predefined variables:

These are variables whose value is predefined and they can be used anywhere in
the workflow description file, without having to be defined on the command line:
{{ SESSION }} : replaced by the started session name

### Step-level variables:

These variables can be present anywhere in the steps description part of the
workflow description file.
They are instanciated when running a step, if the user defines their value
on the command line through the "**--define**" option of the "**iosea-wf run**"
command (see syntax above).

#### Step-level predefined variables:

These are variables whose value is predefined and they can be used anywhere in
the steps description part of the workflow description file, without having to
be defined on the command line:
{{ STEP }} : replaced by the currently running step name

## Trouble shooting:

### If Flash Accelerators do not use the Slurm burst buffer Lua plugin:

1. To check a burst buffer status:
```
   scontrol show burst | grep <service_name>
Name=<service_name> CreateTime=2023-04-26T10:47:52 Pool=(null) Size=50GB State=staged-in UserID=<username>(<uid>)
```

State=staged-out means deallocated
State=staged-in means still allocated

No output means deallocated too

2. To remove a burst buffer "by hand":
```
   srun --bb "SBB destroy_persistent Name=<service_name> /bin/true
```

### If Flash Accelerators use the Slurm burst buffer Lua plugin (i.e. starting from slurm-23.02):

1. To check a burst buffer status:
```
   scontrol show bbstat | grep <service_name>
FA: BB Type=SBB bbid=3589 Name=<service_name> State=staged-in CreateTime=2023-04-26T10:47:52
```

State=staged-out means deallocated
State=staged-in means still allocated

No output means deallocated too

2. To remove a burst buffer "by hand":
```
   srun --bb "#BB_LUA SBB destroy_persistent Name=<service_name> /bin/true
```
