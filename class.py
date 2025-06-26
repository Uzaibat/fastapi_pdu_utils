import index

# Initialize FastAPI app
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state (preserved for compatibility)
message_ew = {
    'messages': [{
        'message': 'Current power utilization :420.5 Watt <br>Proposed power utilization: 405.3<br>Expected power gain: %3.58',
        'show': 1,
        'message_id': 1
    }]
}

# ================== Startup/Shutdown Events ==================
@app.on_event("startup")
async def startup_event():
    """Combined startup event handler"""
    FastAPICache.init(InMemoryBackend())
    app.state.session = aiohttp.ClientSession()
    # set_global_app(app)  # Uncomment if needed

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event handler"""
    await app.state.session.close()

# ================== Optimized Pydantic Models ==================
class LogFile(str, Enum):
    """Log file types with descriptive names"""
    DEFAULT = "default.log"
    MIGRATION = "migration"
    VM_REG = "vm_reg"
    ENVIRONMENTAL = "environmental"
    PREVENTIVE = "preventive"

class BaseApproval(BaseModel):
    """Base model for approval requests"""
    approved: bool

class MigrationDecision(BaseModel):
    """Migration decision model"""
    message_id: int
    status: str

class TimestampModel(BaseModel):
    """Base model with timestamp fields"""
    now_timestamp: str
    future_timestamp: str

class TemperatureModel(TimestampModel):
    """Temperature model with validation"""
    power: str
    flag: str
    env_temp_cur: str
    env_temp_min: str
    power_future_min: str

    @validator('flag')
    def validate_flag(cls, v):
        """Ensure flag is valid"""
        if not v.isdigit():
            raise ValueError("Flag must be numeric")
        return v

class MaintenanceModel(TimestampModel):
    """Maintenance model with power predictions"""
    power: str
    flag: str
    power_future_min: str
    positive_3p: str
    negative_3p: str
    positive_7p: str
    negative_7p: str

class MigrationModel(BaseModel):
    """Migration model with root dictionary"""
    root: Dict[str, dict]

class SaveMigrationModel(BaseModel):
    """Model for saving migration data"""
    status: str
    data: dict

class PowerModel(BaseModel):
    """Base power model"""
    power: float

class GainAfterModel(PowerModel):
    """Model for after-migration gains"""
    past_power: float
    cur_power: float
    prop_power: float
    prop_ratio: float
    actual_ratio: float
    val_ratio: float
    val_difference: float

class GainBeforeModel(PowerModel):
    """Model for before-migration gains"""
    prop_gain: float
    prop_power: float
    cur_power: float

class VmPowerModel(BaseModel):
    """Virtual machine power model"""
    status: str
    name: str
    power: float
    confg: dict

class VMStatus(BaseModel):
    """VM status model"""
    active: List[VmPowerModel]
    inactive: List[VmPowerModel]

class PhysicalMachine(BaseModel):
    """Physical machine model"""
    status: str
    name: str
    power_consumption: float
    vms: VMStatus

class VmPlacementModel(BaseModel):
    """VM placement model"""
    data_center: str
    id: int
    physical_machines: List[PhysicalMachine]

class TimeUnitModel(BaseModel):
    """Base model with time unit configuration"""
    number_of_steps: str = Field(..., regex=r'^\d+$', description="Number of steps")
    script_time_unit: str = Field(..., regex=r'^\d+$', description="Time unit in minutes")
    model_type: str = Field(..., min_length=1, max_length=10, description="Model type")

class EnvInputModel(TimeUnitModel):
    """Environmental input model"""
    pass

class PreventiveInputModel(TimeUnitModel):
    """Preventive input model"""
    pass

class VirtualMachineEstimationModel(BaseModel):
    """VM estimation model with validation"""
    estimation_method: str = Field(
        "indirect",
        description="Estimation Method type",
        regex="^(indirect|direct)$"
    )
    model_type: str = Field(
        "mul_reg",
        min_length=1,
        max_length=10,
        description="Model type"
    )

class MigrationWeightsModel(BaseModel):
    """Migration weights model with validation"""
    power: confloat(ge=0, le=1) = Field(0.25, description="Weight for power factor")
    balance: confloat(ge=0, le=1) = Field(0.25, description="Weight for balance factor")
    overload: confloat(ge=0, le=1) = Field(0.25, description="Weight for overload factor")
    allocation: confloat(ge=0, le=1) = Field(0.25, description="Weight for allocation factor")
    
    @validator('*', pre=True)
    def convert_to_float(cls, v):
        """Convert string values to float"""
        return float(v) if isinstance(v, str) else v

class MigrationAdvicesModel(BaseModel):
    """Migration advice model"""
    migration_method: str = Field(
        "migration_advices_la",
        description="Migration method"
    )
    migration_weights: MigrationWeightsModel = Field(
        default_factory=MigrationWeightsModel,
        description="Migration weights configuration"
    )

class MigrationInputModel(BaseModel):
    """Migration input model with validation"""
    script_time_unit: str = Field(
        "1",
        regex=r'^\d+$',
        description="Time unit in minutes"
    )
    virtual_machine_estimation: VirtualMachineEstimationModel = Field(
        default_factory=VirtualMachineEstimationModel,
        description="VM estimation config"
    )
    migration_advices: MigrationAdvicesModel = Field(
        default_factory=MigrationAdvicesModel,
        description="Migration advice config"
    )
    block_list: List[str] = Field(
        default_factory=list,
        description="List of IP addresses to exclude",
        example=["10.150.1.190"]
    )
    
    @validator('block_list', each_item=True)
    def validate_ip_addresses(cls, v):
        """Validate IP addresses in block list"""
        ip_regex = r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
        if not re.match(ip_regex, v):
            raise ValueError(f"Invalid IP address: {v}")
        return v

class InputDataModel(BaseModel):
    """Consolidated input model"""
    migration: MigrationInputModel
    environmental: EnvInputModel
    preventive: PreventiveInputModel