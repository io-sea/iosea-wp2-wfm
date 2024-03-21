"""Group all routers into a single one.
"""
from wfm_api.routers.service_router import service_router
from wfm_api.routers.session_router import session_router
from wfm_api.routers.step_router import step_router
from wfm_api.routers.config_router import configuration_router

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

# Store all WFM endpoints routes into a single router
wfm_routers = [
        configuration_router,
        service_router,
        session_router,
        step_router
]
