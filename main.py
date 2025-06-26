from utils.queue_manager import Queue
import aiohttp
import asyncio
import httpx
from fastapi import FastAPI, HTTPException, Request, Body, Depends
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, validator
from fabric import Connection
import time
import paramiko
import requests as rq
from sqlalchemy.orm import Session
from models import SnmpMin, Snmp_30sec, Snmp_10, Snmp_cur
from database import get_db
from prom_functions import (
    get_ips, handle_aver_last_min, return_cur, scraper_dict_cr,
    get_actual_snmps_nmin, get_name_snmp, get_snmps_nmin, organize_data,
    return_mixed_part, get_snmps, handle_aver_last_min2, get_ips2
)

app = FastAPI()

# ================== Initialization ==================
# Initialize queues
queues = {
    "maintenance": Queue("maintenance_save.json"),
    "temperature": Queue("temperature_save.json"),
    "migration": Queue("migration_save.json"),
    "gain_before": Queue("gain_before_save.json"),
    "gain_after": Queue("gain_after_save.json"),
    "placement": Queue("gain_vm_placement.json")
}

# Apply queue size constraints
queues["migration"].change_max_amount(1)
queues["gain_before"].change_max_amount(1)
queues["gain_after"].change_max_amount(1)
queues["placement"].change_max_amount(1)

# Centralized configuration
FLASK_API = "http://10.150.1.200:5000"
SNMP_SCRAPER = "http://10.150.1.30:5001"
SSH_CREDS = {
    "user": "ubuntu",
    "connect_kwargs": {"password": "blc2022*"},
    "key_filename": "/home/ubuntu/myenv/myenv/ayposKeypair.pem"
}

# Global state
migration_text = ""
message_ew = {'messages': [{
    'message': 'Current power utilization :420.5 Watt <br>Proposed power utilization: 405.3<br>Expected power gain: %3.58',
    'show': 1,
    'message_id': 1
}]}

# ================== Models ==================
class MaintenanceModel(BaseModel): pass
class TemperatureModel(BaseModel): pass
class MigrationModel(BaseModel): pass
class GainBeforeModel(BaseModel): pass
class GainAfterModel(BaseModel): pass
class VmPlacementModel(BaseModel): pass
class MigrationDecModel(BaseModel): pass
class MigrationMessageModel(BaseModel): pass
class SaveMigrationModel(BaseModel): pass
class InputDataModel(BaseModel): pass
class LogFile: pass  # Enum implementation would go here

class StressRequest(BaseModel):
    vms: List[str] = Field(..., min_items=1, example=["10.150.1.146"])
    level: str = "medium"
    force: bool = False

    @validator('level')
    def validate_level(cls, v):
        if v not in STRESS_LEVELS:
            raise ValueError("Invalid level. Use low/medium/high")
        return v

# ================== Core Functionality ==================
# Stress levels configuration
STRESS_LEVELS = {
    "low": "stress -c 4 -t 30m",
    "medium": "stress -c 8 -t 30m",
    "high": "stress -c 12 -t 30m"
}

def get_connection(ip: str) -> Connection:
    """Create SSH connection with centralized credentials"""
    return Connection(host=ip, **SSH_CREDS)

def is_stress_running(conn: Connection) -> Optional[List[str]]:
    """Check if stress is running on a server"""
    try:
        result = conn.run("pgrep -a stress", hide=True, warn=True)
        return result.stdout.strip().split('\n') if result.ok and result.stdout else None
    except Exception as e:
        raise RuntimeError(f"Stress check failed: {str(e)}")

async def make_async_request(
    url: str,
    method: str = "POST",
    payload: Optional[Dict] = None
) -> Dict:
    """Unified async request handler for all HTTP requests"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, json=payload) as response:
                if response.status != 200:
                    error = await response.text()
                    raise HTTPException(502, detail=f"API error: {error}")
                return await response.json()
    except Exception as e:
        raise HTTPException(500, detail=str(e))

async def execute_stress(vms: List[str], level: str, force: bool = False) -> Dict:
    """Core stress execution logic used by all stress endpoints"""
    command = f"nohup {STRESS_LEVELS[level]} >/dev/null 2>&1 &"
    results = {}
    
    for ip in vms:
        try:
            with get_connection(ip) as conn:
                # Check existing processes
                if not force and is_stress_running(conn):
                    results[ip] = {"status": "skipped", "reason": "Stress already running"}
                    continue
                
                # Force stop if requested
                if force:
                    conn.run("pkill -9 -f stress", hide=True)
                
                # Start new process
                conn.run(command, hide=True)
                results[ip] = {"status": "started", "command": command}
        except Exception as e:
            results[ip] = {"error": str(e)}
    
    return results

async def migrate_stream_helper(version: int, run_migration: bool, request: Request = None):
    """
    Unified migration streaming logic
    Supports all 4 original versions with same behavior
    """
    if not run_migration:
        if version == 4:
            queues["migration"].empty_queue()
            queues["gain_before"].empty_queue()
        else:
            queues["migration"].queue.pop(0)
            queues["migration"].length = len(queues["migration"].queue)
        return {"message": "Migration declined"}
    
    try:
        if version == 3:  # httpx implementation
            async with httpx.AsyncClient(timeout=httpx.Timeout(None)) as client:
                async with client.stream('POST', f"{FLASK_API}/run-migration") as response:
                    if response.status_code != 200:
                        error_detail = await response.aread()
                        raise HTTPException(response.status_code, detail=f"Flask API error: {error_detail}")

                    async def generate():
                        async for chunk in response.aiter_bytes():
                            print(chunk.decode('utf-8', errors='replace').strip())
                            yield chunk

                    return StreamingResponse(
                        generate(),
                        media_type="text/event-stream",
                        headers={
                            'Cache-Control': 'no-cache',
                            'Connection': 'keep-alive',
                            'X-Accel-Buffering': 'no'
                        }
                    )
        else:  # aiohttp implementations
            timeout = aiohttp.ClientTimeout(total=None)
            connector = aiohttp.TCPConnector(force_close=False, limit=None)
            headers = {"Accept": "text/event-stream"}
            
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers
            ) as session:
                async with session.post(f"{FLASK_API}/run-migration") as response:
                    if response.status != 200:
                        error = await response.text()
                        raise HTTPException(response.status, detail=f"Flask error: {error}")

                    if version == 1:
                        async def event_stream():
                            async for chunk in response.content.iter_chunks():
                                if request and await request.is_disconnected():
                                    break
                                yield chunk[0]
                        
                        return StreamingResponse(
                            event_stream(),
                            media_type=response.headers.get('Content-Type', 'text/event-stream')
                        )
                    else:  # Versions 2 and 4
                        async def generate():
                            async for chunk in response.content.iter_any():
                                print(chunk.decode('utf-8').strip())
                                yield chunk
                        
                        media_type = "text/event-stream" if version == 4 else response.headers.get('content-type', 'text/plain')
                        return StreamingResponse(
                            generate(),
                            media_type=media_type,
                            headers={
                                'Cache-Control': 'no-cache',
                                'Connection': 'keep-alive'
                            }
                        )
    except Exception as e:
        raise HTTPException(500, detail=str(e))

# ================== Endpoints ==================
# ----- Migration Endpoints -----
@app.post("/prom/migration/decisions")
async def migration_decision_v1(request: Request, run_migration: bool = False):
    return await migrate_stream_helper(1, run_migration, request)

@app.post("/prom/migration/decisions2")
async def migration_decision_v2(run_migration: bool):
    return await migrate_stream_helper(2, run_migration)

@app.post("/prom/migration/decisions3")
async def migration_decision_v3(run_migration: bool):
    return await migrate_stream_helper(3, run_migration)

@app.post("/prom/migration/decisions4")
async def migration_decision_v4(run_migration: bool = False):
    return await migrate_stream_helper(4, run_migration)

# ----- Temperature Endpoint -----
@app.post("/prom/temperature/decisions")
async def temperature_decision(approval: bool):
    if not approval:
        return {"message": "Temperature change declined"}
    
    data = queues["temperature"].get_data(1)
    if not data:
        return {"message": "No temperature data available"}
    
    try:
        flag_value = data[0]['flag']
        try:
            flag_value = int(flag_value)
            response = await make_async_request(
                url=f"{FLASK_API}/process_temp/{flag_value}",
                method="GET"
            )
            return {"status": "SUCCESS", "response": response}
        except ValueError:
            return {"message": "Still not time yet or it is fine"}
    except IndexError:
        return {"message": "Invalid temperature data format"}

# ----- Stress Endpoints -----
@app.get('/prom/stress/high')
async def start_high_stress():
    """High stress on compute1"""
    results = await execute_stress(["10.150.1.35"], "high")
    return {"status": "success", "results": results}

@app.get('/prom/stress/mid')
async def start_mid_stress():
    """Medium stress on compute1 and compute2"""
    results = await execute_stress(["10.150.1.35", "10.150.1.34"], "medium")
    return {"status": "success", "results": results}

@app.get('/prom/stress/low')
async def start_low_stress():
    """Low stress on compute1"""
    results = await execute_stress(["10.150.1.35"], "low")
    return {"status": "success", "results": results}

@app.post("/stress/start")
async def start_stress(request: StressRequest):
    """Unified stress start endpoint"""
    results = await execute_stress(request.vms, request.level, request.force)
    return {"results": results}

@app.post("/stress/stop")
async def stop_stress(vms: List[str] = Body(..., min_items=1)):
    """Stop stress on multiple servers"""
    results = {}
    for ip in vms:
        try:
            with get_connection(ip) as conn:
                processes = is_stress_running(conn)
                if not processes:
                    results[ip] = {"status": "already_stopped"}
                    continue
                
                conn.run("pkill -9 -f stress", hide=True)
                
                # Verify stopped
                stopped = False
                for _ in range(3):
                    if not is_stress_running(conn):
                        stopped = True
                        break
                    time.sleep(0.5)
                
                results[ip] = {
                    "status": "stopped" if stopped else "partial",
                    "killed": len(processes),
                    "previous_pids": processes
                }
        except Exception as e:
            results[ip] = {"status": "failed", "error": str(e)}
    return {"results": results}

@app.get('/prom/stress/stop')
async def stop_stress_legacy():
    """Original stress stop implementation for compute2"""
    try:
        with get_connection("10.150.1.34") as conn:
            conn.run("bash /home/ubuntu/stop_sc.sh", hide=True)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

# ----- Data Endpoints -----
@app.get("/prom/aver")
async def get_last_10_min_average_data():
    ip_dict = get_ips()
    return {server: handle_aver_last_min(ip_dict[server]) for server in ip_dict}

@app.get("/prom/cur")
async def get_current_prometheus_data():
    ip_dict = get_ips()
    return {server: return_cur(ip_dict[server]) for server in ip_dict}

@app.get("/prom/snmps/cur")
async def get_computes_snmp_cur_data():
    return scraper_dict_cr()

@app.get("/prom/snmp/cur")
async def get_snmp_cur_data(db: Session = Depends(get_db)):
    compute3_power = get_name_snmp()['compute3']
    post = db.query(Snmp_cur).first()
    titles = ["voltage", "current", "pf", "energy", "power"]
    values = post.snmpdata.split(',')[1:]
    
    return {
        **{titles[i]: round(float(values[i]), 4) for i in range(len(titles)-1)},
        "power": str(compute3_power)
    }

# ----- Queue Operations -----
@app.post("/prom/push/{queue_name}")
async def push_to_queue(queue_name: str, data: BaseModel):
    """Unified queue push endpoint"""
    if queue_name not in queues:
        raise HTTPException(404, detail="Invalid queue name")
    
    data_dict = data.dict()
    queues[queue_name].push(data_dict)
    
    # Special handling for gain_after
    if queue_name == "gain_after":
        queues["migration"].empty_queue()
    
    return data_dict

@app.get("/prom/get/{queue_name}")
async def get_queue_data(queue_name: str, n: int = None):
    """Unified queue data retrieval"""
    if queue_name not in queues:
        raise HTTPException(404, detail="Invalid queue name")
    
    data = queues[queue_name].get_data(n if n else 1)
    
    # Special handling for single-item queues
    if queue_name in ["migration", "gain_before", "gain_after", "placement"]:
        return data[0] if data else {}
    
    return {"data": data}

# ----- Special Endpoints -----
@app.get('/prom/migration/message')
async def get_migration_messages():
    return message_ew

@app.post('/prom/save/migration')
async def save_migration(data: SaveMigrationModel):
    print(data)
    return {"status": "success"}

@app.post('/prom/push/migration_text')
async def save_migration_text(data: MigrationMessageModel):
    global message_ew
    gain_dict = data.data
    migration_text = (
        f"Current Power: {gain_dict['power_cur']}<br>"
        f"Proposed Power: {gain_dict['pow_prop']}<br>"
        f"Gain: {gain_dict['gain']}"
    )
    message_ew = {'messages': [{
        'message': migration_text,
        'show': 1,
        'message_id': 1
    }]}
    return {"status": "updated"}

# ----- Physical Machine Access -----
@app.get("/prom/phy_mac/{iplast}")
async def get_phy_machine_data(iplast: str):
    """Original paramiko-based implementation"""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            f"10.150.1.{iplast}",
            username="ubuntu",
            password="blc2022*"
        )
        script = """
import psutil
memory = psutil.virtual_memory()
disk = psutil.disk_usage('/')
cpu_count = psutil.cpu_count()
total_memory = memory.total
available_memory = memory.available
used_memory = memory.used
free_memory = memory.free
disk_total = disk.total
disk_used = disk.used
disk_free = disk.free
print(f"{'total_memory': {total_memory}, 'available_memory': {available_memory}, 'used_memory': {used_memory}, 'free_memory': {free_memory}, 'disk_total': {disk_total}, 'disk_used': {disk_used}, 'disk_free': {disk_free}}")
        """
        stdin, stdout, stderr = ssh.exec_command(f"python3 -c '{script}'")
        data = stdout.read().decode('utf-8')
        ssh.close()
        return JSONResponse(content=data)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

# ----- Monitoring Control -----
@app.post("/prom/start_monitoring")
async def start_monitoring(inputs: InputDataModel):
    """Reset queues and start monitoring"""
    for queue in queues.values():
        queue.empty_queue()
    response = rq.post(f"{FLASK_API}/start", json=inputs.dict()).json()
    return response

@app.post("/prom/stop_monitoring")
async def stop_monitoring():
    response = rq.post(f"{FLASK_API}/stop").json()
    return response

@app.get("/prom/monitoring_status")
async def monitoring_status():
    response = rq.get(f"{FLASK_API}/status").json()
    return response

@app.get("/prom/monitoring_logs")
async def monitoring_logs(script_name: LogFile = LogFile.default):
    params = {} if script_name == LogFile.default else {"script_name": script_name.value}
    response = rq.get(f"{FLASK_API}/logs", params=params)
    
    if response.status_code == 200:
        return response.json()
    raise HTTPException(response.status_code, detail=response.json().get("error", "Error fetching logs"))

# ----- Machine Details -----
@app.get('/prom/pm_mac_details')
async def get_physical_machine_details():
    """Physical machine details with RAM conversion"""
    response = rq.get(f"{SNMP_SCRAPER}/get-pm-conf")
    res = response.json()
    for i in res:
        res[i]['idle consumption'] = 114
        res[i]["memory_mb"] = float(res[i]["memory_mb"]) / 1024
    return {"res": res}

@app.get('/prom/vm_mac_details')
async def get_virtual_machine_details():
    """Virtual machine details with RAM conversion"""
    response = rq.get(f"{SNMP_SCRAPER}/get-vm-conf")
    res = response.json()['result']
    for i in res:
        res[i]['ram'] = float(res[i]['ram']) / 1024
    return {"res": res}
