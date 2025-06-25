from utils.queue_manager import Queue



queue_maintenance = Queue("maintenance_save.json")
queue_temperature = Queue("temperature_save.json")
queue_migration = Queue("migration_save.json")
#queue_migration_prime = Queue("migration_prime_save.json")
queue_migration.change_max_amount(1)
#queue_migration_prime.change_max_amount(1)
queue_gain_before = Queue("gain_before_save.json")
queue_gain_before.change_max_amount(1)
queue_gain_after = Queue("gain_after_save.json")
queue_gain_after.change_max_amount(1)
queue_placement = Queue("gain_vm_placement.json")
queue_placement.change_max_amount(1)

# queue = Queue()
# mainintenance_queue = Queue()
# migration_queue = Queue()
# Now you can use the class


async def make_async_request(
    url: str,
    method: str = "POST",
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Generic aiohttp request handler with error handling.
    
    Args:
        url: Target URL
        method: HTTP method (GET/POST/PUT/DELETE)
        payload: Request body data
        headers: Custom headers
        timeout: Timeout in seconds
        
    Returns:
        JSON response as dict
        
    Raises:
        HTTPException: On request failure
    """
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.request(
                method=method,
                url=url,
                json=payload,
            ) as response:
                
                if response.status != 200:
                    error_text = await response.text()
                    raise HTTPException(
                        status_code=502,
                        detail=f"Remote API error ({response.status}): {error_text}"
                    )
                
                return await response.json()
                
    except ValueError as e:
        raise HTTPException(400, detail=f"Invalid request data: {str(e)}")
    except aiohttp.ClientError as e:
        raise HTTPException(503, detail=f"Service unavailable: {str(e)}")
    except Exception as e:
        raise HTTPException(500, detail=f"Unexpected error: {str(e)}")

@app.post("/prom/migration/decisions")
async def start_migration(request: Request, run_migration: bool = False):
    if not run_migration:
        return {"message": "Migration declined"}

    try:
        # Create a persistent client session
        timeout = aiohttp.ClientTimeout(total=None)  # No timeout
        connector = aiohttp.TCPConnector(force_close=False, limit=None)
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"Accept": "text/event-stream"}
        ) as session:
            # Start the request to Flask
            async with session.post(
                "http://10.150.1.200:5000/run-migration",
                raise_for_status=False
            ) as response:

                # Handle non-200 responses
                if response.status != 200:
                    error = await response.text()
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"Flask API error: {error}"
                    )

                # Stream the response chunks
                async def event_stream():
                    try:
                        async for chunk in response.content.iter_chunks():
                            if await request.is_disconnected():
                                print("Client disconnected")
                                break
                            yield chunk[0]
                    except asyncio.CancelledError:
                        print("Stream cancelled")
                        raise
                    except Exception as e:
                        print(f"Stream error: {str(e)}")
                        raise

                return StreamingResponse(
                    event_stream(),
                    media_type=response.headers.get('Content-Type', 'text/event-stream')
                )

    except aiohttp.ClientError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Connection to Flask API failed: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )

@app.post("/prom/migration/decisions4")
async def start_migration(run_migration: bool = False):
    if not run_migration:
        queue_migration.empty_queue()
        queue_gain_before.empty_queue()
        return {"message": "Migration declined"}


    async def forward_stream():
        async with aiohttp.ClientSession() as session:
            async with session.post("http://10.150.1.200:5000/run-migration") as response:
                async for chunk in response.content.iter_any():
                    yield chunk

    return StreamingResponse(
        forward_stream(),
        media_type="text/event-stream"
    )


@app.post("/prom/temperature/decisions")
async def get_flag(approval: bool):
    # Check if the boolean value is True
    if approval:
        # Retrieve data from queue
        data = queue_temperature.get_data(1)
        
        # Extract the 'flag' value
        flag_value = data[0]['flag'] #data[0]['flag']
        try:
            # Try to convert the flag to an integer
            flag_value = int(flag_value)
            #return {"flag": flag_value}
            try:
                # Use the generic function
                flask_response = await make_async_request(
                    url=f"http://10.150.1.200:5000/process_temp/{flag_value}",
                    method="GET",
                )
                return {
                    "status": "SUCCESS",
                    "response": flask_response
                }
            except HTTPException as e:
                # Custom handling if needed
                raise e
        except ValueError:
            # If it can't be converted to int, return a custom message
            return {"message": "Still not time yet or it is fine"}
    else:
        return {"message": "Temperature change declined"}
import httpx
@app.post("/prom/migration/decisions3")
async def start_migration(run_migration: bool):
    if not run_migration:
        queue_migration.queue.pop(0)
        queue_migration.length = len(queue_migration.queue)
        return {"message": "Migration declined"}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(None)) as client:
            # Use stream=True to get streaming response
            async with client.stream(
                'POST',
                "http://10.150.1.200:5000/run-migration"
            ) as response:
                if response.status_code != 200:
                    error_detail = await response.aread()
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Flask API returned error: {error_detail}"
                    )

                async def generate():
                    async for chunk in response.aiter_bytes():
                        print(chunk.decode('utf-8', errors='replace').strip())
                        yield chunk

                return StreamingResponse(
                    generate(),
                    media_type="text/event-stream",  # Important for SSE
                    headers={
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive',
                        'X-Accel-Buffering': 'no'  # Disable buffering
                    }
                )

    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Connection error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@app.post("/prom/migration/decisions2")
async def start_migration(run_migration: bool):
    if not run_migration:
        queue_migration.queue.pop(0)
        queue_migration.length = len(queue_migration.queue)
        return {"message": "Migration declined"}
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
            async with session.post("http://10.150.1.200:5000/run-migration") as response:
                if response.status != 200:
                    error_detail = await response.text()
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"Flask API returned error: {error_detail}"
                    )
                
                # Create a streaming response that forwards the content
                async def generate():
                    async for chunk in response.content.iter_any():
                        # Print to FastAPI console (optional)
                        print(chunk.decode('utf-8').strip())
                        yield chunk
                
                return StreamingResponse(
                    generate(),
                    media_type=response.headers.get('content-type', 'text/plain'),
                    headers={
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                )
    
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=503, detail=f"Connection error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
                #return await response.json()
#                return StreamingResponse(
#                    response.content.iter_chunked(1024),
#                    media_type="text/plain"
#                )
#    except Exception as e:
#        raise HTTPException(status_code=500, detail=str(e))

async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

@app.get("/prom/aver")
async def get_last_10_min_average_data():
    
    ip_dict = get_ips()
    data_holder = {}
    
    for server in ip_dict:
        data_holder[server] = handle_aver_last_min(ip_dict[server])

    return data_holder


@app.get('/prom/snmp/cur_power')
async def get_current_powers_computes():
    return get_snmps()


@app.get('/prom/stress/high')
async def start_high_stress():
    try:
        node_ip = "10.150.1.35"

        with Connection(host=node_ip, user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:

            result = c.run("nohup stress -c 12 -t 10m >/dev/null 2>&1 &", hide=True)
            return {"status": "success"}

    except Exception as e:
        print(e)


@app.get('/prom/stress/mid')
async def start_mid_stress():
    try:
        node_ip = "10.150.1.35"

        with Connection(host=node_ip, user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:

            result = c.run("nohup stress -c 8 -t 10m >/dev/null 2>&1 &", hide=True)
            return {"status": "success"}

        with Connection(host="10.150.1.34", user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:

            result = c.run("nohup stress -c 8 -t 10m >/dev/null 2>&1 &", hide=True)
            return {"status": "success"}

    except Exception as e:
        print(e)


@app.get('/prom/stress/low')
async def start_low_stress():
    try:
        node_ip = "10.150.1.35"

        with Connection(host=node_ip, user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:

            result = c.run("nohup stress -c 4 -t 10m >/dev/null 2>&1 &", hide=True)
            return {"status": "success"}

    except Exception as e:
        print(e)
#***********************************************************

SSH_USER = "ubuntu"
SSH_KEY_PATH = "/home/ubuntu/myenv/myenv/ayposKeypair.pem"
STRESS_LEVELS = {
    "low": "stress -c 4 -t 30m",
    "medium": "stress -c 8 -t 30m",
    "high": "stress -c 12 -t 30m"
}



class VMsRequest(BaseModel):
    vms: List[str]

class StressRequest(BaseModel):
    vms: List[str] = Field(..., min_items=1, example=["10.150.1.146"])
    level: str = "medium"
    force: bool = False

    @validator('level')
    def validate_level(cls, v):
        if v not in STRESS_LEVELS:
            raise ValueError("Invalid level. Use low/medium/high")
        return v

def get_connections(ips: List[str]) -> Group:
    """Correct way to create a SerialGroup with connections"""
    if not ips:
        raise ValueError("At least one VM must be specified")

    connection_strings = [f"{SSH_USER}@{ip}" for ip in ips]
    return Group(
        *connection_strings,
        connect_kwargs={
            "key_filename": SSH_KEY_PATH
        }
    )

def is_stress_running(connection: Connection) -> bool:
    """Check if stress is running, returns None if not running"""
    try:
        result = connection.run("pgrep -a stress", hide=True, warn=True, in_stream=False)
        if result.ok and result.stdout.strip():
            return result.stdout.strip().split('\n')
        return None
    except Exception as e:
        raise RuntimeError(f"Failed to check stress on {connection.host}: {str(e)}")

async def run_in_threadpool(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func, *args)


@app.post("/stress/status")
async def check_stress_status(vms: List[str] = Body(..., min_items=1)):
    """
    Check stress status on servers
    
    Expects: ["10.150.1.146", "10.150.1.182"] (raw JSON array)
    Returns: { "10.150.1.146": { "is_running": bool, "processes": Optional[List[str]] }, ... }
    """
    try:
        group = get_connections(vms)
        group = await run_in_threadpool(get_connections, vms)
        results = {}

#        for connection in group:
#            try:
#                pids = is_stress_running(connection)
#                results[connection.host] = {
#                    "is_running": pids is not None,
#                    "processes": pids
#                }
#            except Exception as e:
#                results[connection.host] = {
#                    "error": str(e),
#                    "is_running": False,
#                    "processes": None
#                }
        def check_single(connection):
            try:
                pids = is_stress_running(connection)
                return {
                    "is_running": pids is not None,
                    "processes": pids
                }
            except Exception as e:
                return {
                    "error": str(e),
                    "is_running": False,
                    "processes": None
                }
        # Run checks in parallel
        futures = [run_in_threadpool(check_single, conn) for conn in group]
        results_list = await asyncio.gather(*futures)
        
        # Map results to hostnames
        for conn, result in zip(group, results_list):
            results[conn.host] = result

        return results
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/stress/start")
async def start_stress(request: StressRequest):
#async def start_stress(vms: List[str], level: str = "medium", force: bool = False):
    """Start stress with safety check"""
#    if level not in STRESS_LEVELS:
#        return {"error": "Invalid level. Use low/medium/high"}
    group = get_connections(request.vms)
    command = f"nohup {STRESS_LEVELS[request.level]} >/dev/null 2>&1 &"
    results = {}
    for connection in group:
        # Get the list of running PIDs
        pids = is_stress_running(connection)
        if not request.force and pids:
            results[connection.host] = {
                "status": "skipped",
                "reason": "Stress already running",
                "processes": pids
            }
        else:
            # Kill existing stress if forcing
            if request.force:
                connection.run("pkill -9 -f stress", hide=True, warn=True, in_stream=False)
            # Start new stress
            result = connection.run(command, hide=True, in_stream=False)
            # Split PIDs by newline and store as a list
            new_pids = is_stress_running(connection)  # Get new PIDs
            results[connection.host] = {
                "status": "started",
                "command": command,
                "pid": new_pids # Return PIDs as a list
            }
    return {"results": results}

def verify_stress_stopped(connection: Connection):
    """Check if stress is completely stopped"""
    for _ in range(3):  # 3 retries
        pids = is_stress_running(connection)
        if not pids:
            return True
        connection.run("pkill -9 -f stress", hide=True,in_stream=False)
        time.sleep(0.5)  # Brief pause between kill attempts
    return False

@app.post("/stress/stop")
async def stop_stress(vms: List[str] = Body(..., min_items=1)):
    """Stop stress on servers with parallel optimized killing"""
    try:
        group = get_connections(vms)
        results = {}

        async def process_host(connection):
            try:
                # Initial status check
                #pids = await asyncio.get_event_loop().run_in_executor(
                #    None, is_stress_running, connection
                #)
                pids = await run_in_threadpool(is_stress_running, connection)
                if not pids:
                    results[connection.host] = {"status": "already_stopped"}
                    return
                
                # Kill all processes at once
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: connection.run("pkill -9 -f stress", hide=True, warn=True,in_stream=False)
                )
                
                # Verify
                #stopped = await asyncio.get_event_loop().run_in_executor(
                #    None, verify_stress_stopped, connection
                #)

                stopped = await run_in_threadpool(verify_stress_stopped, connection)
                results[connection.host] = {
                    "status": "stopped" if stopped else "partial",
                    "killed": len(pids),
                    "previous_pids": pids
                }
            except Exception as e:
                results[connection.host] = {
                    "status": "failed",
                    "error": str(e)
                }

        await asyncio.gather(*[process_host(conn) for conn in group])
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get('/prom/stress/stop')
async def stop_stress():
    try:
        node_ip = "10.150.1.34"

        with Connection(host=node_ip, user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:

            result = c.run("bash /home/ubuntu/stop_sc.sh", hide=True)
            return {"status": "success"}

    except Exception as e:
        print(e)


@app.get('/prom/snmp/n_min_aver_power/{i}')
async def get_last_n_min_powers_computes(i: int):

    data = get_actual_snmps_nmin(i)
    modified_data = {
    key: {
        new_key.replace("pdu_", ""): value for new_key, value in data[key].items()
    }
    for key in data.keys()
    }
    pup_p = {key:modified_data[key] for key in modified_data} # if key != 'compute3'}
    for key in pup_p:
        pup_p[key]['pf'] = 0.92324562
        pup_p[key]['energy'] /= 10
    
    pup_p['compute4']['energy'] /= 3 
    return pup_p


@app.get("/prom/snmp/min")
async def get_snmp_min_aveage(db: Session = Depends(get_db)):

    data = db.query(SnmpMin).all()
    empty_d = {}
    titles = ["time_stamp", "voltage", "current", "pf", "energy", "power"]
    print(data[0]) 
    l = str(data[0].snmpdata).split(',')

    print(len(l), len(titles))
    for i in range(len(titles)):
        empty_d[titles[i]] = l[i]

    return empty_d


@app.get('/prom/snmpcsv/hour/{computename}')
async def get_snmp_csv_data_hour(computename: str):

    compute = computename.split('&')[0]
    n = int(computename.split('&')[1])
    
    get_snmps_nmin(n, 'h', compute)

    return FileResponse(f'/home/ubuntu/myenv/myenv/{compute}.csv')


@app.get('/prom/snmpcsv/day/{computename}')
async def get_snmp_csv_data_day(computename: str):
    
    compute = computename.split('&')[0]
    n = int(computename.split('&')[1])

    get_snmps_nmin(n, 'd', compute)
    return FileResponse(f'/home/ubuntu/myenv/myenv/{compute}.csv')


@app.get('/prom/snmpcsv/minute/{computename}')
async def get_snmp_csv_data_minute(computename:str):
    compute = computename.split('&')[0]
    n = int(computename.split('&')[1])

    get_snmps_nmin(n, compute_name=compute)
    
    return FileResponse(f'/home/ubuntu/myenv/myenv/{compute}.csv')


@app.get("/prom/mixed/aver30")
async def get_30_sec_average_data_mixed(db: Session = Depends(get_db)):

    posts = db.query(Snmp_30sec).all()
    initi = [0,0,0,0, 0]
    emp = {}
    titles = ["voltage", "current", "pf", "energy", "power"]

    for post in posts:
        adata = post.snmpdata
        # print(post.snmpdata)
        l = adata.split(',')
        td = l[0]
        l = l[1:len(l)]
        
        # t = post.timedata

        for index in range(len(l)):
            initi[index] += float(l[index])
            # c=index

    for i in range(len(initi)):

        emp[titles[i]] = round(initi[i]/len(posts), 4)

    print(len(initi), len(l))
    print(len(post.snmpdata))
    # print(initi)
    # return emp # {"data": initi}
    # posts = db.query
    # return return_mixed_part()
    dc = {**return_mixed_part(), **emp}
    # dc["ts"] = td
    return dc # {**return_mixed_part(), **emp}["ts"] = td
    # didnt work out neither
    # return(emp.update(return_mixed_part()))
    
    # return return_mixed_part() | emp
    # naah for Python 3.9, too lazy to upgrade

@app.get("/prom/aver/lastmin/{minu}")
#@cache(expire=10)  # Cache the result for 10 seconds
@cache(expire=20)  # Cache for 60 seconds globally
async def get_last_n_min_average_data(minu: int):
    ip_dict = await get_ips2()
    tasks = [handle_aver_last_min2(ip_dict[server], False, int(minu)) for server in ip_dict]
    results = await asyncio.gather(*tasks)

    return {server: results[i] for i, server in enumerate(ip_dict)}
"""
@app.get("/prom/aver/lastmin2/{minu}")
#@cache(expire=20)  # Cache for 60 seconds
async def get_last_n_min_average_data(minu: int):

    ip_dict = get_ips()
    data_holder = {}
    #print(ip_dict)
    
    idle_dc = {}

    for server in ip_dict:
        # if server == '10.150.1.34':
            
            # continue

        data_holder[server] = handle_aver_last_min(ip_dict[server], False, int(minu))
    
    return data_holder
"""
"""
    try:
        node_ip = "10.150.1.30"

        with Connection(host=node_ip, user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:

            result = c.run("python3 -c '{}'".format(script_vm_mac_details), hide=True)

            res = eval(result.stdout)
            res = res['result']
            for i in res:
                res[i]['ram'] = float(res[i]['ram']) / 1024

    except Exception as e:
        print(e)
    
    for i in res:
        if 'ip' in res[i].keys():

            ip = res[i]["ip"]
            name = i
            if ip in data_holder.keys():

                idle_dc[name] = data_holder[ip]

    idle_dc['compute3'] = data_holder['10.150.1.34']
    idle_dc['compute2'] = data_holder['10.150.1.33']
    
    return idle_dc
"""

@app.get("/prom/cur")
async def get_current_prometheus_data():

    ip_dict = get_ips()
    data_holder = {}

    for server in ip_dict:
        data_holder[server] = return_cur(ip_dict[server])

    return data_holder


@app.get("/prom/snmps/cur")
async def get_computes_snmp_cur_data():
    return scraper_dict_cr()



@app.get("/prom/snmp/cur")
async def get_snmp_cur_data(db: Session = Depends(get_db)):
    
    compute3_power = get_name_snmp()['compute3']
    post = db.query(Snmp_cur).first()
    emp = {}
    titles = ["voltage", "current", "pf", "energy", "power"]

    l = post.snmpdata.split(',')
    l = l[1:len(l)]

    for i in range(len(l)):

        emp[titles[i]] = round(float(l[i]), 4)
    
    emp['power'] = str(compute3_power)
    return emp


@app.get("/prom/snmp")
async def get_snmp_10_min_data(db: Session = Depends(get_db)):
     
    posts = db.query(Snmp_10).all()
    initi = [0,0,0,0, 0]
    emp = {}
    titles = ["voltage", "current", "pf", "energy", "power"]

    for post in posts:
        adata = post.snmpdata
        # print(post.snmpdata)
        l = adata.split(',')
        l = l[1:len(l)]
        t = post.timedata
        
        for index in range(len(l)):
            initi[index] += float(l[index])
            # c=index
   
    for i in range(len(initi)):

        emp[titles[i]] = round(initi[i]/len(posts), 4)

    # print(initi)
    return emp # {"data": initi}
    # posts = db.query

"""
@app.get("/prom/last/{stri}")
def get_last_n_time_average_data(stri: str):

    match = re.search(r'day=(\d+);hour=(\d+);minute=(\d+)', stri)

    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
    else:
        print("Pattern not found in the endpoint string.")


@app.get("/prom/interval/{stri}")
def get_an_interval_average_data(stri: str):
    
    start_match = re.search(r'start=(\d{2}):(\d{2})_(\d{2})_(\d{2})_(\d{4})', input_string)
    end_match = re.search(r'end=(\d{2}):(\d{2})_(\d{2})_(\d{2})_(\d{4})', input_string)
    if start_match and end_match:
        start_hour = int(start_match.group(1))
        start_minute = int(start_match.group(2))
        start_day = int(start_match.group(3))
        start_month = int(start_match.group(4))
        start_year = int(start_match.group(5))

        end_hour = int(end_match.group(1))
        end_minute = int(end_match.group(2))
        end_day = int(end_match.group(3))
        end_month = int(end_match.group(4))
        end_year = int(end_match.group(5))

    ...
"""

@app.get("/prom/smoothaver/{nhour}")
async def get_smooth_n_hour_data(nhour: int):
    return handle_aver_last_min(0, last10=None, go_hour_back=nhour)


@app.get("/prom/day/{nday}")
async def get_last_n_day_csv_data_and_download(nday:int):
    cmd = ["ls ../out"]
    organize_data(nday)

    # pass
    return FileResponse("/home/ubuntu/out/thefactory/1.csv")


import aiohttp
import asyncio


async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

@app.get('/prom/pm_mac_details')
@cache(expire=20)  # Cache the result for 20 seconds
async def get_physical_mac_details():
    try:
        res = await fetch_data("http://10.150.1.30:5001/get-pm-conf")

        for i in res:
            res[i]['idle consumption'] = 114
            res[i]["memory_mb"] = float(res[i]["memory_mb"]) / 1024

        return {"res": res}

    except Exception as e:
        print(e)
        return {"error": str(e)}

@app.get('/prom/vm_mac_details')
@cache(expire=20)  # Cache the result for 20 seconds
async def get_mac_details():
    try:
        res = await fetch_data("http://10.150.1.30:5001/get-vm-conf")

        res = res['result']
        for i in res:
            res[i]['ram'] = float(res[i]['ram']) / 1024

        return {"res": res}

    except Exception as e:
        print(e)
        return {"error": str(e)}







#@app.get('/prom/pm_mac_details')
#async def get_physical_mac_details():
#
#    try:
#        res = rq.get("http://10.150.1.30:5001/get-pm-conf").json()
#
#        for i in res:
#            res[i]['idle consumption'] = 114
#            res[i]["memory_mb"] = float(res[i]["memory_mb"]) / 1024
            # res[i]['memory_mb'] = float(res[i]['memory_mb']
            
#        return {"res": res}

#    except Exception as e:
#        print(e)

#@app.get('/prom/vm_mac_details')
#async def get_mac_details():
#    res = rq.get("http://10.150.1.30:5001/get-vm-conf").json()
        
#    res = res['result']
#    for i in res:
#        res[i]['ram'] = float(res[i]['ram']) / 1024

#    return {"res": res}

@app.post("/prom/start_monitoring")
async def start_monitoring_scripts(inputs: InputDataModel):
    """Start scripts by sending a request to the remote Flask server"""
    print(inputs.dict())
    queue_maintenance.empty_queue()
    queue_temperature.empty_queue()
    queue_migration.empty_queue()
    queue_gain_before.empty_queue()
    queue_gain_after.empty_queue()
    response = rq.post("http://10.150.1.200:5000/start", json=inputs.dict()).json()
    return response

@app.post("/prom/stop_monitoring")
async def stop_monitoring_scripts():
    """Stop scripts by sending a request to the remote Flask server"""
    response = rq.post("http://10.150.1.200:5000/stop").json()
    return response

@app.get("/prom/monitoring_status")
async def check_monitoring_status():
    """Check script status on remote server"""
    response = rq.get("http://10.150.1.200:5000/status").json()
    return response

@app.get("/prom/monitoring_logs")
async def check_monitoring_logs(script_name: LogFile = LogFile.default):
    """Check script logs on the Flask server by calling Flask API."""
    
    try:
        if script_name == LogFile.default:
            # If 'default' is chosen, make the request without the log_file param
            response = rq.get(f"http://10.150.1.200:5000/logs")  # No log filename parameter
        else:
            # Otherwise, send the log_file parameter to Flask
            response = rq.get(f"http://10.150.1.200:5000/logs", params={"script_name": script_name.value })
            #params = {"script_name": script_name.value}

        #response = rq.get(f"http://10.150.1.200:5000/logs", params=params)
        if response.status_code == 200:
            return response.json()  # Return logs as JSON
        else:
            raise HTTPException(status_code=response.status_code, detail=response.json().get("error", "Error fetching logs"))
    except rq.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error calling Flask API: {str(e)}")

@app.get('/prom/monitoring')
async def get_monitoring_conf():
    mon_conf = rq.get("http://10.150.1.30:5001/get-moni-conf").json()
    return {
        "data_center": "BLC",
        "id": 1,
        "status": "open",
        "optimization_space": mon_conf.get("optimization_space", {})
    }
    return mon_conf


"""
@app.get("/prom/phy_mac/{iplast}")
async def get_psutil_script_data_phy_machine(iplast):
    try:
        # Establish an SSH connection to the remote node
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        node_ip = "10.150.1." + iplast

        ssh.connect(node_ip, username="ubuntu", password="blc2022*")
        # Define the script to run on the remote node
        script = '''
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

print(f"{{'total_memory': {{total_memory}}, 'available_memory': {{available_memory}}, 'used_memory': {{used_memory}}, 'free_memory': {{free_memory}}, 'disk_total': {{disk_total}}, 'disk_used': {{disk_used}}, 'disk_free': {{disk_free}}}}}")
        '''

        # Execute the script on the remote node
        stdin, stdout, stderr = ssh.exec_command(script_vm_mac_details)
        data = stdout.read().decode('utf-8')
        ssh.close()

        return JSONResponse(content=data)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

        """


# return success in post models
@app.post('/prom/push/maintenance_data')
async def push_chart_data_maintenance(data: MaintenanceModel):
    queue_maintenance.push(data)

    return data


@app.post('/prom/push/temperature_data')
async def push_chart_data_temperature(data: TemperatureModel):
    queue_temperature.push(data)

    return data


@app.post('/prom/migration2/decisions')
async def get_migration_decisions(data: MigrationDecModel):
    print(data)
    print(type(data))
    for message in message_ew['messages']:
        if data.message_id == message['message_id'] and data.status == 'decline':
            message['show'] = 0
            print(message_ew)
migration_text = ""
message_ew = {'messages': [{'message': 'Current power utilization :420.5 Watt <br>Proposed power utilization: 405.3<br>Expected power gain: %3.58', 'show': 1, 'message_id': 1}]}


# MigrationMessageModel
@app.post('/prom/push/migration_text')
async def save_new_migration(data: MigrationMessageModel):
    global message_ew
    gain_dict = data.data
    # message_ew = {'messages': [{'message': 'Current power utilization :420.5 Watt <br>Proposed power utilization: 405.3<br>Expected power gain: %3.58', 'show': 1, 'message_id': 1}]}
    
    migration_text = "Current Power: " + str(gain_dict["power_cur"]) + "<br>" + "Proposed Power: " + str(gain_dict["pow_prop"]) + "<br>" + "Gain: " + str(gain_dict["gain"])
    message_ew = {'messages': [{'message': migration_text, 'show': 1, 'message_id': 1}]}

    print(data)
    return data


@app.post('/prom/save/migration')
async def save_new_migration(data: SaveMigrationModel):
    print(data)

@app.post('/prom/push/migration_data')
async def push_chart_data_migration(data: MigrationModel):
    data = data.dict()
    queue_migration.push(data)

    return data
# return success
@app.post('/prom/push/gain_before')
async def push_chart_data_data_gain_before(data: GainBeforeModel):
    data = data.dict()
    queue_gain_before.push(data)

    return data

@app.post('/prom/push/gain_after')
async def push_chart_data_gain_after(data: GainAfterModel):
    data = data.dict()
    queue_gain_after.push(data)
    queue_migration.empty_queue()
    return data

@app.post('/prom/push/vm_placement')
async def push_chart_data_vm_placement(data: VmPlacementModel):
    data = data.dict()
    queue_placement.push(data)

    return data

#@app.post('/prom/push/migration_data_prime')
#async def push_chart_data_migration_prime(data: MigrationPrimeModel):
#    queue_migration_prime.push(data)
#    print(data)
#
#    return data


@app.get('/prom/get_chart_data/temperature/{n}')
async def get_n_temperature_chart_data(n: int):
    data = queue_temperature.get_data(n)
    # print(data)
    return {"data": data}


@app.get('/prom/get_chart_data/temperature')
async def get_all_temperature_chart_data():
    data = queue_temperature.get_data()
    # print(data)
    return {"data": data}


@app.get('/prom/get_chart_data/maintenance/{n}')
async def get_n_maintenance_chart_data(n: int):
    data = queue_maintenance.get_data(n)
    # print(data)
    return {"data": data}


@app.get('/prom/get_chart_data/maintenance')
async def get_all_maintenance_chart_data():
    data = queue_maintenance.get_data()
    # print(data)
    return {"data": data}


#@app.get('/prom/get_chart_data/migration/{n}')
#async def get_n_migration_chart_data(n: int):
#    data = queue_migration.get_data(n)
    #data = {"data": data}
#    return data


@app.get('/prom/get_chart_data/migration')
async def get_all_migration_chart_data():
    data = queue_migration.get_data()
    try:
        # Use parse_obj_as to validate the data into the correct MigrationModel format
        parsed_data = parse_obj_as(List[MigrationModel], data)  # Ensures data matches MigrationModel
    except ValidationError as e:
        return {"error": "Data validation failed", "details": e.errors()}
    
    # Return the first item if available, or the entire list of valid items
    return parsed_data[0] if parsed_data else parsed_data

@app.get('/prom/get_chart_data/gain_after')
async def get_all_chart_data_gain_after():
    data = queue_gain_after.get_data()
    
    try:
        # Try parsing data into the GainAfterModel list
        parsed_data = parse_obj_as(List[GainAfterModel], data)  
    except ValidationError as e:
        return {"error": "Data validation failed", "details": e.errors()}
    
    # Return the first parsed item if available, or the entire parsed data
    return parsed_data[0] if parsed_data else parsed_data


@app.get('/prom/get_chart_data/gain_before')
async def get_all_chart_data_gain_before():
    data = queue_gain_before.get_data()
    
    try:
        # Try parsing data into the GainBeforeModel list
        parsed_data = parse_obj_as(List[GainBeforeModel], data)  
    except ValidationError as e:
        return {"error": "Data validation failed", "details": e.errors()}
    
    # Return the first parsed item if available, or the entire parsed data
    return parsed_data[0] if parsed_data else parsed_data

@app.get('/prom/get_chart_data/vm_placement') 
async def get_all_chart_data():
    # Retrieve data from the queue
    data = queue_placement.get_data(1)
    
    # If the data has been serialized as dictionaries, convert them back to models if needed
    #try:
        #parsed_data = [VmPlacementModel(**item) for item in data]  # Recreate model instances from the dictionary
    #except Exception as e:
    #    return {"error": "Error processing data", "details": str(e)}
    try:
        # Use parse_obj_as to validate the data into the correct model format
        parsed_data = parse_obj_as(List[VmPlacementModel], data)  # Ensures data matches VmPlacementModel
    except ValidationError as e:
        return {"error": "Data validation failed", "details": e.errors()}
    return parsed_data[0] if parsed_data else parsed_data

@app.get('/prom/migration/message')
async def get_migration_messages():
    return message_ew
    # return {'messages': [{'message': 'Virtual machine s88 can be migrated to compute3 from compute2. It will possibly save 14.6 watts meaning %2.7 energy saving.', 'show': 1, 'message_id': 1}, {'message': 'Virtual machine Redmine can be migrated to compute2 from compute3. It will possibly save 4.0 watts meaning %0.7 energy saving.', 'show': 1, 'message_id': 2}]}


#@app.get("/prom/get_chart_data/migrationprime")
#async def get_migration_primer():
#    
#    data = queue_migration_prime.get_data()
#    return {"data": data}


@app.get("/prom/phy_mac/{iplast}")
async def get_psutil_script_data_phy_machine(iplast):
    try:
        node_ip = "10.150.1." + iplast
        with Connection(host=node_ip, user="ubuntu", connect_kwargs={"password": "blc2022*"}) as c:
            #c.run("pip install psutil", hide=True)
            result = c.run("python3 -c '{}'".format(script_phy_mac), hide=True)
            # c.run("pip install psutil", hide=True)
            return {"res":result.stdout}
    except Exception as e:
        return {"error": f"An error occurred: {e}"}
            

# @app.get('/prom/random_migrate')
# def random_migrator(migrate_model: RandomMigrate):
#     ...


