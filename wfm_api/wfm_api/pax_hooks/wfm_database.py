"""This module defines a PAX hook for manipulation of the WFM database
"""
import contextlib
from loguru import logger
from fastapi import Request
from wfm_api.utils.database.wfm_database import WFMDatabase

__copyright__ = """
Copyright (C) 2022 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""


@contextlib.asynccontextmanager
async def wfm_database_hook(
    container,
):
    """A hook providing a database instance in application state."""
    # Check if database is enabled, and if not, yield an empty value.
    if container.settings.database.enabled:
        database = WFMDatabase(**container.settings.database.dict())
    else:
        database = None
    logger.info(f"Opening database with in {container.settings}")
    # Create new database instance using path from settings
    # Attach database to application state
    container.app.state.database = database
    # Let the application run (I.E, signal startup complete)
    # The yielded value is not used by the application itself
    # So yielding the database is equivalent to yielding None when application is running
    # However, yielding a value helps writing unit tests.
    try:
        yield database
    # Always clean resources on application shutdown
    finally:
        logger.info(f"Closing database in {container.settings}")


def wfm_database(request: Request) -> WFMDatabase:
    """Access the database from a Starlette/FastAPI request"""
    return request.app.state.database  # type: ignore[no-any-return]
