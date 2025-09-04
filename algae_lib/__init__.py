"""
BarAlgae Data Infrastructure - Shared Library

This package contains shared utilities for data processing, S3 operations,
database connections, and FlowCam data handling.
"""

__version__ = "1.0.0"
__author__ = "Amit Sasson"

from .s3_io import S3Manager
from .db_io import DatabaseManager
from .flowcam_utils import FlowCamProcessor

__all__ = ["S3Manager", "DatabaseManager", "FlowCamProcessor"]
