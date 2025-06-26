# Standard Library Imports
import asyncio
import json
import re
import subprocess
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

# Third-Party Imports

import aiohttp
import psutil
import requests as rq
from fabric import Connection
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from pydantic import BaseModel, Field, ValidationError, validator

# Local Imports (preserving all original functionality)
from database import *
from handler_funcs import *
from informative_scripts import *