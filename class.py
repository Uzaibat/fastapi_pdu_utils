import index
app = FastAPI()
origins = ["*"]

message_ew = {'messages': [{'message': 'Current power utilization :420.5 Watt <br>Proposed power utilization: 405.3<br>Expected power gain: %3.58', 'show': 1, 'message_id': 1}]}

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    FastAPICache.init(
        InMemoryBackend()
    )

# Main endpoint
@app.on_event("startup")
async def startup():
    import aiohttp
    app.state.session = aiohttp.ClientSession()

    set_global_app(app)  # make app available to metrics.py

@app.on_event("shutdown")
async def shutdown():
    await app.state.session.close()




class LogFile(str, Enum):
    default = "default.log"
    migration = "migration"
    vm_reg = "vm_reg"
    environmental = "environmental"
    preventive = "preventive"

class ApprovalRequest(BaseModel):
    approved: bool

class MigrationDecModel(BaseModel):
    message_id: int
    status: str


class TemperatureModel(BaseModel):
    power: str
    flag: str
    env_temp_cur: str
    now_timestamp: str
    future_timestamp: str
    env_temp_min: str
    power_future_min: str

migration_text = ""


class MigrationMessageModel(BaseModel):
    data: dict


class MaintenanceModel(BaseModel):
    power: str
    flag: str
    now_timestamp: str
    future_timestamp: str
    power_future_min: str
    positive_3p: str
    negative_3p: str
    positive_7p: str
    negative_7p: str


class MigrationModel(RootModel):
    #data: dict
    root: Dict[str, dict]
    # ratio: dict


#class MigrationPrimeModel(BaseModel):
#    runningPms: list
#    offPms: list


class SaveMigrationModel(BaseModel):
    status: str
    data: dict

class GainAfterModel(BaseModel):
    past_power: float
    cur_power: float
    prop_power: float
    prop_ratio: float
    actual_ratio: float
    val_ratio: float
    val_difference: float

class GainBeforeModel(BaseModel):
    prop_gain: float
    prop_power: float
    cur_power: float

# Represents a virtual machine (VM)
class VmPowerModel(BaseModel):
    status: str
    name: str
    power: float
    confg: dict

# Represents the status of VMs, including active and inactive VMs
class VMStatus(BaseModel):
    active: List[VmPowerModel]
    inactive: List[VmPowerModel]

# Represents a physical machine with power consumption and associated VMs
class PhysicalMachine(BaseModel):
    status: str
    name: str
    power_consumption: float
    vms: VMStatus

# Represents the data center, containing physical machines
class VmPlacementModel(BaseModel):
    data_center: str
    id: int
    physical_machines: List[PhysicalMachine]

#class VmPlacementModel(BaseModel):
#    sc_so: dict
#    vm_power: Dict[str, float]

class EnvInputModel(BaseModel):
    number_of_steps: str = Field(..., pattern=r'^\d+$', description="Number of steps (numeric only)", example="3")
    script_time_unit: str = Field(..., pattern=r'^\d+$', description="Time unit in minutes (e.g., '1' or '5')", example="1")
    model_type: str = Field(..., min_length=1, max_length=10, description="Model type (string with 1-10 characters)", example="lstm")


class PreventiveInputModel(BaseModel):
    number_of_steps: str = Field(..., pattern=r'^\d+$', description="Number of steps (numeric only)", example="3")
    script_time_unit: str = Field(..., pattern=r'^\d+$', description="Time unit in minutes (e.g., '1' or '5')", example="1")
    model_type: str = Field(..., min_length=1, max_length=10, description="Model type (string with 1-10 characters)", example="lstm")

class VirtualMachineEstimationModel(BaseModel):
    estimation_method: str = Field(
        "indirect",
        min_length=1,
        max_length=10,
        description="Estimation Method type (string with 1-10 characters)",
        example="indirect"
    )
    model_type: str = Field(
        "mul_reg",
        min_length=1,
        max_length=10,
        description="Model type (string with 1-10 characters)",
        example="mul_reg"
    )

    @validator('estimation_method')
    def validate_estimation_method(cls, v):
        valid_methods = ["indirect", "direct"]
        if v not in valid_methods:
            raise ValueError(f"estimation_method must be one of {valid_methods}")
        return v

class MigrationWeightsModel(BaseModel):
    power: str = Field("0.25", pattern=r'^0(\.\d+)?$|^1(\.0+)?$', description="Weight for power factor")
    balance: str = Field("0.25", pattern=r'^0(\.\d+)?$|^1(\.0+)?$', description="Weight for balance factor")
    overload: str = Field("0.25", pattern=r'^0(\.\d+)?$|^1(\.0+)?$', description="Weight for overload factor")
    allocation: str = Field("0.25", pattern=r'^0(\.\d+)?$|^1(\.0+)?$', description="Weight for allocation factor")

class MigrationAdvicesModel(BaseModel):
    migration_method: str = Field(
        "migration_advices_la",
        description="Migration method",
        example="migration_advices_la"
    )
    migration_weights: MigrationWeightsModel = Field(
        default_factory=lambda: MigrationWeightsModel(),
        description="Migration weights configuration"
    )

class MigrationInputModel(BaseModel):
    script_time_unit: str = Field(
        "1",
        pattern=r'^\d+$',
        description="Time unit in minutes",
        example="1"
    )
    virtual_machine_estimation: VirtualMachineEstimationModel = Field(
        default_factory=lambda: VirtualMachineEstimationModel(),
        description="VM estimation config"
    )
    migration_advices: MigrationAdvicesModel = Field(
        default_factory=lambda: MigrationAdvicesModel(),
        description="Migration advice config"
    )
    block_list: List[str] = Field(
        default_factory=list,
        description="List of IP addresses to not include in migration",
        example=["10.150.1.190"]
    )

#class MigrationInputModel(BaseModel):
#    script_time_unit: str = Field(..., pattern=r'^\d+$', description="Time unit in minutes(e.g., '20' or '60')", example="20")
#    estimation_method: str = Field(..., min_length=1, max_length=10, description="Estimation Method type (string with 1-10 characters)", example="indirect")
#    model_type: str = Field(..., min_length=1, max_length=10, description="Model type (string with 1-10 characters)", example="mul_reg")
#    migration_method: str = Field(..., description="Migration method (string)", example="migration_advices_la")
#    block_list: List[str] = Field(
#        default=[],
#        description="List of IP addresses to not include in migration",
#        example=["10.150.1.190", "10.150.1.193"]
#    )

    #@validator('block_list', each_item=True)
    #def validate_ip_addresses(cls, v):
    #    ip_regex = r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    #    if not re.match(ip_regex, v):
    #        raise 

class InputDataModel(BaseModel):
    migration: MigrationInputModel
    environmental: EnvInputModel
    preventive: PreventiveInputModel
