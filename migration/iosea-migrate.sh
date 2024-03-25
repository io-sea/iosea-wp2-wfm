#!/bin/bash

usage()
{
	cat <<-EOF
	Usage:
		$0 -S <size> [options] <sources> <destination>
		$0 -l
		$0 -W <session>

	Options:
		-l              list running migration sessions
		-b              start migration in the background
		-W <session>    wait completion of a background session
		-N <nodes>      allocate several compute nodes and do the copy in parallel
		-S <size>       specify the size of the dataset to create. For instance, -S 20GiB

	EOF
}

ROOTDIR="${HOME}"/.ioseamigration
mkdir -p "${ROOTDIR}"


make_workflow()
{
cat <<EOF > $tmpdir/wf.yml
workflow:
  name: Migration

services:
  - name: gbf1
    type: NFS
    attributes:
      namespace: $namespace
      mountpoint: $mountpoint
      storagesize: $storagesize
      datanodes: 1
      location: dp-cn

steps:
  - name: step_copy
    command: "sbatch $tmpdir/sbatch.sh"
    services:
      - name: gbf1
EOF
}

make_sbatch()
{
cat <<EOF > $tmpdir/sbatch.sh
#!/bin/bash
#SBATCH --partition=dp-cn -N $nodes -o $sbatch_out_file -J migration

# Set up the environment required or mpifileutils
. /p/scratch/iosea/migration/mpifileutils/env.sh

status=\$(iosea-wf status)
step=\$( echo "$status" | grep " \$SLURM_JOB_ID " | awk '{ print \$4 }')

echo This is Iosea migration session $session step $step

env | grep SLURM

declare -a sourcefiles
EOF

for i in $(seq 0 $((nfiles - 1))); do
	echo sourcefiles[$i]=\""${allfiles[$i]}"\" >> $tmpdir/sbatch.sh
done

cat <<EOF >> $tmpdir/sbatch.sh

srun $tmpdir/copier.sh --progress 1 "\${sourcefiles[@]}" "$cpdest" | tee $tmpdir/copier_out | $tmpdir/status_updater.sh

rc=\${PIPESTATUS[0]}

echo "Copy status is \$rc"
exit \$rc

EOF

chmod +x $tmpdir/sbatch.sh

}

make_copier()
{
	local parentdir=$(dirname $mountpoint)
	cat <<EOF > $tmpdir/copier.sh
#!/bin/bash

n=\$(findmnt -n --target "$parentdir" | grep -v autofs | awk '{ print \$2 }')
m=\$(findmnt -n --target "$mountpoint" | grep -v autofs | awk '{ print \$2 }')
host=\$(uname -n)

if [ "\$m" = "\$n" ]; then
	echo "Device for $parentdir is \$n" >&2
	echo "Device for $mountpoint is \$m" >&2
	echo "$mountpoint does not appear to be mounted" >&2
	mount > $tmpdir/mounts.\$host
	exit 1
fi

df -h $mountpoint

echo Running dcp \$@
dcp \$@ | tee $tmpdir/dcp_out

rc=\${PIPESTATUS[0]}
if [ "\$rc" -ne 0 ]; then
	echo dcp failed
	exit 1
fi

echo Sync on \$host:
( time bash -c 'sync;sync;sync' ) 2>&1
df -h $mountpoint

outdir=\$(cat $tmpdir/dcp_out | sed -n 's/^[^ ]* Copying to //p')

cat $tmpdir/dcp_out | sed -n 's/^[^ ]* Walking //p' > $tmpdir/infiles
cat $tmpdir/infiles | sed 's|.*/|'"\$outdir"'/|' > $tmpdir/outfiles

cat $tmpdir/infiles | xargs -i find {} -type f | xargs stat -c %s | sort -g | grep -v "^0" > $tmpdir/sizes_in
cat $tmpdir/outfiles | xargs -i find {} -type f | xargs stat -c %s | sort -g | grep -v "^0" > $tmpdir/sizes_out

if ! cmp $tmpdir/sizes_in $tmpdir/sizes_out; then
	echo Size check failed
	exit 1
fi

EOF

	chmod +x $tmpdir/copier.sh
}

# This script is responsible for sending the progress of the copy to the WFM at regular intervals.
# It parses the ouput of dcp which has this format: 
# Copied 446.548 MiB (76%) in 4.169 secs (107.119 MiB/s) 1 secs left ...
# It also produces $tmpdir/stats
make_status_updater()
{
cat <<EOF > $tmpdir/status_updater.sh
report()
{
	echo REPORT: \$(date) : \$*
	iosea-wf update -j \$SLURM_JOB_ID -p "\$*"
}

status="Preparing files"
report "\$status"

cut -d ' ' -f 2- | while read line; do 
	if [[ "\$line" == Copied* ]] || [[ "\$line" == Syncing* ]]; then
		report "\$line"
	fi
	if [[ "\$line" == "Copy rate"* ]]; then
		echo "\$line" > $tmpdir/stats
	fi
done
EOF
chmod +x $tmpdir/status_updater.sh
}



get_sessions()
{
	iosea-wf status | tail -n +2 | awk '{ print $1 }' | grep '^iosea-migration-'
}

# migration-3zQvwvx                                  Migration                                          active
get_session_status()
{
	local session="$1"
	local out=$(iosea-wf status -s "$session" 2>&1)
      
	if grep -qsE '(No session with name|not found in the WFDB)' <<< "$out"; then
		echo Session "$session" not found, assuming it is stopped >&2
		echo stopped
	elif grep -qs "ERROR" <<< "$out"; then
		echo "$out" >&2
		echo Failed to get status of session "$session">&2
		echo error
	else
		echo "$out" | tail -n +2 | awk '{ print $3 }'
	fi
}

wait_session_status()
{
	local session="$1"
	shift
	local status
	local prev_status=none
	echo Waiting for session "$session" to reach one of these states: "$*"
	while :; do
		#capture_global_status
		status=$(get_session_status "$session")
		if [ "$status" = "$prev_status" ]; then 
			echo -n .
		else
			if [ "$prev_status" != none ]; then
				echo
			fi
			echo -n Current session status "$status"
		fi
		if [ "$status" = teardown ]; then
			echo
			echo Error: session is in teardown
			return 1
		fi
		for s in $*; do
			if [ "$status" = "$s" ]; then
				break 2
			fi
		done
		sleep 2
		prev_status="$status"
	done
	echo
}

get_step_status()
{
	local session="$1"
	local step="$2"
	local out
	out=$(iosea-wf status -s "$session" -t "$step" 2>/dev/null)
	if [ -z "$out" ]; then
		echo unavailable
		return 0
	fi
	echo "$out" | tail -n +2 | awk '{ print $3 }' 
}

get_step_progress()
{
	local session="$1"
	local step="$2"
	iosea-wf status -s "$session" -t "$step" | tail -n +2 | awk '{out=$5; for(i=6;i<=NF;i++){out=out" "$i}; print out}'
}

# After the step has completed, check with slurm that it completed successfully
# returns 'COMPLETED', or something else (bad news then)
get_session_step_slurm_status()
{
	local session="$1"
	local jobid=$(iosea-wf status -s "$session" -T | tail -1 | awk '{ print $4 }')
	sacct -j ${jobid} -o state | tail -1 | tr -d ' '
}

wait_step_status()
{
	local session="$1"
	local step="$2"
	shift 2
	local status
	local prev_status=none

	echo Waiting for step "$step" to reach one of these states: "$*"
	while :; do
		status=$(get_step_status "$session" "$step")
		if [ "$status" = "running" ]; then
			progress=$(get_step_progress "$session" "$step")
			echo "Step is running, progress: $progress"
		else
			if [ "$status" = "$prev_status" ]; then 
				echo -n .
			else
				if [ "$prev_status" != none ]; then
					echo
				fi
				echo -n Current step status "$status"
			fi
		fi
		for s in $*; do
			if [ "$status" = "$s" ]; then
				break 2
			fi
		done
		sleep 2
		prev_status="$status"
	done
	echo
}

indent() 
{
	sed 's/^/|- /'
}

list_sessions()
{
	local session
	local step=step_copy
	for session in $(get_sessions); do
		local step_status=""
		local session_status=$(get_session_status "$session")
		if [ "$session_status" = "active" ]; then
			step_status=$(get_step_status "$session" "$step")
			status="step_$step_status"
		else
			status="session_$session_status"
		fi
		echo + Migration in session "$session" is in state "$status"
		if [ "$step_status" = "running" ]; then
			progress=$(get_step_progress "$session" "$step")
			echo "Progress: $progress"
		fi | indent
		if [ "$step_status" = "stopped" ]; then
			echo "Copy done, checking session..."
			local jobstatus=$(get_session_step_slurm_status "$session")
			if [ "$jobstatus" == "COMPLETED" ]; then
				echo "Copy done, stopping session..."
				iosea-wf stop -s "$session" 2>&1
			else
				echo Error: An error occured while copying data. Migration failed
			
			fi
		fi | indent
	done
}

wait_completion=yes
wait_completion_of=""
start_session=no
nodes=1
storagesize=""
show_help=no

# COMMAND LINE PARSING
while [ "$1" ]; do
	case "$1" in
		-l) list_sessions; exit $? ;;
		-b) wait_completion=no; shift ;;
		-w) wait_completion=yes; shift ;;
		-W) wait_completion_of=$2; shift 2;;
		-N) nodes=$2; shift 2;;
		-S) storagesize=$2; shift 2;;
		-h | --help) show_help=yes; shift;;
		*) start_session=yes; break;;
	esac
done

if [ $show_help = yes -o -z "$storagesize" -o -z "$1" ]; then
	usage
	exit 0
fi

step=step_copy

if [ "$start_session" = yes ]; then
	# Of all file-like parameters, the destination namespace is the last one.
	declare -a allfiles=( "$@" )
	nfiles=${#allfiles[@]}
	if echo "${allfiles[0]}" | grep -qs :; then
		direction=from
		nsparam="$(echo ${allfiles[0]} | cut -d : -f 1)"
		pattern="$(echo ${allfiles[0]} | cut -d : -f 2-)"
		if [ $nfiles -ne 2 ]; then
			echo 'Expected: <input_dataset:pattern> <output_dir>'
			exit 1
		fi
		destdir="${allfiles[1]}"
	else
		direction=to
		nsparam="${allfiles[$nfiles]}"
		(( nfiles-- )) # don't count destination as an input file
		nsparam="${allfiles[$nfiles]}"
		unset allfiles[$nfiles]
	fi


	prefix=$( cut -s -d '@' -f 1 <<< "$nsparam" )
	postfix=$( cut -s -d '@' -f 2- <<< "$nsparam" )
	if [ -z "$postfix" ]; then
		namespace_prefix=""
		namespace_file="$nsparam"
	else
		namespace_prefix="$prefix""@"
		namespace_file="$postfix"
	fi
	namespace_dir=$( dirname "$namespace_file" )
	namespace_name=$( basename "$namespace_file" )
	mkdir -p "$namespace_dir"
	namespace_dir=$(readlink -f "$namespace_dir") # make it absolute
	namespace="$namespace_prefix$namespace_dir/$namespace_name"

	tmpdir=$(mktemp -d ${ROOTDIR}/iosea-migration-XXXXXXX)
	session=$( echo "$tmpdir" | sed 'sX.*/XX')
	chmod og+rx "$tmpdir" # Root needs this to make the mount of NFS

	mountpoint="$tmpdir"/mnt
	mkdir -p "$mountpoint"

	echo tmpdir is "$tmpdir"
	echo session is "$session"
	echo mountpoint is "$mountpoint"
	echo namespace is "$namespace"

	sbatch_out_file="$tmpdir"/step_out.txt

	if [ $direction = to ]; then
		cpdest="$mountpoint/"
	else
		cpdest="$destdir"
		allfiles=( "$mountpoint/$pattern" )
		nfiles=1
	fi


	make_workflow
	make_copier
	make_status_updater
	make_sbatch

	cmdout=$(iosea-wf start -w "$tmpdir/wf.yml" -s "$session" 2>&1)
	rc=$?
	if grep -qs ERROR <<< "$cmdout"; then
		rc=1
	fi
	if [ $rc -ne 0 ]; then
		echo "$cmdout"
		echo Failed to start session. Abort.
		exit 1
	fi

	echo Session "$session" started

	if ! wait_session_status "$session" "active"; then
		echo Failed to start services. Abort.
		iosea-wf stop -s "$session"
		exit 1
	fi


	if ! iosea-wf run -s "$session" -t "$step"; then
		echo Failed to start copy. Abort.
		iosea-wf stop -s "$session"
		exit 1
	fi


	if [ "$wait_completion" = yes ]; then
		wait_completion_of="$session"
	fi
fi

if [ "$wait_completion_of" ]; then
	session="$wait_completion_of"
	tmpdir="${ROOTDIR}/$session"

	session="$wait_completion_of"

	echo Waiting for step to start
	wait_step_status "$session" "$step" running stopped


	echo Waiting for step to complete
	wait_step_status "$session" "$step" stopped

	echo Stopping session
	iosea-wf stop -s "$session"

	echo Checking copy status
	jobstatus=$(get_session_step_slurm_status "$session")
	if [ "$jobstatus" != "COMPLETED" ]; then
		echo Error: An error occured while copying data. Migration failed
		exit 1
	fi


	echo Waiting for session to be stopped

	if ! wait_session_status "$session" "stopped"; then
		echo Error: An error occured while stopping the service. Migration failed
		exit 1
	fi

	echo Copy stats:
	cat $tmpdir/stats
fi
