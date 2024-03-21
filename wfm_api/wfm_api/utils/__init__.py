"""This package contains the different utils for the WFM API.
"""
from wfm_api.utils.ephemeral_services.sbb_ephemeral_service import SBBEphemeralService
from wfm_api.utils.ephemeral_services.no_ephemeral_service import NOEphemeralService
from wfm_api.utils.ephemeral_services.gbf_ganesha_ephemeral_service import GBFGaneshaEphemeralService
from wfm_api.utils.ephemeral_services.dasi_ephemeral_service import DASIEphemeralService
from wfm_api.utils.job_managers.slurm_job_manager import SlurmJobManager
from wfm_api.utils.resource_managers.iosea_resource_manager import IOSEAResourceManager
from wfm_api.utils.resource_managers.no_resource_manager import NOResourceManager

__copyright__ = """
Copyright (C) Bull S.A.S.
"""

EPHEMERAL_SERVICES = {
        'SBB': SBBEphemeralService,
        'NFS': GBFGaneshaEphemeralService,
        'DASI': DASIEphemeralService,
        'NONE': NOEphemeralService
}

JOB_MANAGERS = {
        'SLURM': SlurmJobManager
}

RESOURCE_MANAGERS = {
        'IOSEA': IOSEAResourceManager,
        'NONE': NOResourceManager
}
