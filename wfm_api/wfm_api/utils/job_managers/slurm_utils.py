"""
Utility routines to be called when Slurm is used as a job scheduler.
"""
from enum import Enum

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class SlurmJobStatus(str, Enum):
    """Enumeration class for the actual slurm job status.
    """
    BOOT_FAIL = 'BOOT_FAIL'
    CANCELLED = 'CANCELLED'
    COMPLETED = 'COMPLETED'
    CONFIGURING = 'CONFIGURING'
    COMPLETING = 'COMPLETING'
    DEADLINE = 'DEADLINE'
    FAILED = 'FAILED'
    NODE_FAIL = 'NODE_FAIL'
    OUT_OF_MEMORY = 'OUT_OF_MEMORY'
    PENDING = 'PENDING'
    PREEMPTED = 'PREEMPTED'
    RUNNING = 'RUNNING'
    RESV_DEL_HOLD = 'RESV_DEL_HOLD'
    REQUEUE_FED = 'REQUEUE_FED'
    REQUEUE_HOLD = 'REQUEUE_HOLD'
    REQUEUED = 'REQUEUED'
    RESIZING = 'RESIZING'
    REVOKED = 'REVOKED'
    SIGNALING = 'SIGNALING'
    SPECIAL_EXIT = 'SPECIAL_EXIT'
    STAGE_OUT = 'STAGE_OUT'
    STOPPED = 'STOPPED'
    SUSPENDED = 'SUSPENDED'
    TIMEOUT = 'TIMEOUT'


class WFMJobStatus(str, Enum):
    """Enumeration class for the slurm job status.
    Used to convert between a job status as returned by slurm squeue command
    and a status known to StepStatus.
    """
    BOOT_FAIL = 'STOPPED'
    CANCELLED = 'STOPPED'
    COMPLETED = 'STOPPED'
    CONFIGURING = 'STARTING'
    COMPLETING = 'STOPPING'
    DEADLINE = 'STOPPED'
    FAILED = 'STOPPED'
    NODE_FAIL = 'STOPPED'
    OUT_OF_MEMORY = 'STOPPED'
    PENDING = 'STARTING'
    PREEMPTED = 'STOPPED'
    RUNNING = 'RUNNING'
    RESV_DEL_HOLD = 'STARTING'
    REQUEUE_FED = 'SUSPENDED'
    REQUEUE_HOLD = 'SUSPENDED'
    REQUEUED = 'STOPPING'
    RESIZING = 'RUNNING'
    REVOKED = 'STOPPED'
    SIGNALING = 'RUNNING'
    SPECIAL_EXIT = 'STOPPED'
    STAGE_OUT = 'STOPPING'
    STOPPED = 'STOPPED'
    SUSPENDED = 'SUSPENDED'
    TIMEOUT = 'STOPPED'
