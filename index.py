from fastapi import FastAPI, Depends, Request ,HTTPException, Body
import re
from handler_funcs import *
# from decimal import Decimal
from database import *
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
import json
import os
import subprocess
# from fastapi.responses import JSONResponse
# import paramiko
import psutil
from fabric import Connection, Group, SerialGroup
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from informative_scripts import *
import requests as rq
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, RootModel, parse_obj_as, ValidationError, Field, validator
from enum import Enum
import aiohttp
import asyncio
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache

