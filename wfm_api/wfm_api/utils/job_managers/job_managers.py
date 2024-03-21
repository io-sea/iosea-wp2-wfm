"""
The goal of the JobManager class is to provide an abstraction of job managers.
It provides the methods that must be implemented when adding support
for a new job manager through the Workflow Manager.
"""
from abc import abstractmethod
from typing import Any, Dict, List

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

DEFAULT_COMMAND = '/bin/true'

class JobManager:
    """
    Represents a job manager (defined as an abstract interface).

    This class provides methods to:
        - Translate between a job status as managed by the WFM and a job status as managed by
          the RM (to_rm_job_status)
        - Translate between a job status as managed by the RM and a job status as managed by
          the WFM (to_wfm_job_status)
        - Get a job status (get_job_status).
        - Cancel a job (cancel_job).
        - Get all locations available to the user (get_usable_locations)
        - Combine a set of status strings into a single one aimed for output
          (combine_step_status_for_output)
        - Combine a set of status strings into a single one aimed for stopping the associated
          job (combine_step_status_for_stopping)
    """
    def __init__(self):
        """Initializes the instance variables
        """
        self.job_mgr_name = ''
        self.cancel_command = DEFAULT_COMMAND
        self.job_state_cmd = DEFAULT_COMMAND

    @abstractmethod
    def to_rm_job_status(self, status: str) -> str:
        """Returns the Slurm view of a WFM job status.

        Args:
            status (str): the job status

        Returns:
            str: the slurm job status
        """

    @abstractmethod
    def to_wfm_job_status(self, status: str) -> str:
        """Returns the WFM view of a slurm job status.

        Args:
            status (str): the job status

        Returns:
            str: the internally managed job status
        """

    @abstractmethod
    def get_job_status(self, jobid: int) -> str:
        """Gets a job status.

        Args:
            jobid (int): the job id

        Returns:
            str: The job status
        """

    @abstractmethod
    def cancel_job(self, jobid: int) -> int:
        """Cancels a job

        Args:
            jobid (int): the job id to cancel

        Returns:
            int: The launched command return code. 0 on success
        """

    @abstractmethod
    def get_usable_locations(self) -> List[Dict[str, Any]]:
        """Gets all partition names that can be used

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The partitions names
        """

    @abstractmethod
    def combine_step_status_for_output(self, status: str) -> str:
        """Combine a set of blank-separated status strings into a single one,
        aimed for output.

        Args:
            status: the step status (potentially several blank-separated status strings)

        Returns:
            str: The combined status
        """

    @abstractmethod
    def combine_step_status_for_stopping(self, status: str) -> str:
        """Combine a set of blank-separated status strings into a single one,
        aimed for stopping the associated job.

        Args:
            status: the step status (potentially several blank-separated status strings)

        Returns:
            str: The combined status
        """
