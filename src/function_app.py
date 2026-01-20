import json
import logging
import os
from threading import Lock
from typing import Optional

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Constants for the Azure Blob Storage container, file, and blob path
_SNIPPET_NAME_PROPERTY_NAME = "snippetname"
_SNIPPET_PROPERTY_NAME = "snippet"
_BLOB_PATH = "snippets/{mcptoolargs." + _SNIPPET_NAME_PROPERTY_NAME + "}.json"
_OCC_NAME_PROPERTY_NAME = "occupation"
_JOB_CODE_PROPERTY_NAME = "job_code"
_RANK_CODE_PROPERTY_NAME = "rank"
_OCC_PATH = "occupation/{mcptoolargs." + _OCC_NAME_PROPERTY_NAME + "}.json"

# ============================================================================
# Data Cache for Occupations - Loaded once and reused across tool invocations
# ============================================================================
class OccupationDataCache:
    """
    A thread-safe cache for occupation data loaded from Azure Blob Storage.
    All occupation files (up to 20) are loaded once at first access and reused 
    across all tool invocations within the same function instance.
    """
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._occupation_data = {}  # Dict: occupation_name -> parsed JSON data
                    cls._instance._is_loaded = False
                    cls._instance._blob_service_client = None
        return cls._instance
    
    def _get_blob_client(self):
        """Lazily initialize the blob service client.
        Supports both connection string and identity-based authentication.
        """
        if self._blob_service_client is None:
            # Try connection string first
            connection_string = os.environ.get("AzureWebJobsStorage")
            if connection_string and not connection_string.startswith("__"):
                logging.info("Using connection string for blob storage")
                self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                # Use identity-based authentication
                blob_service_uri = os.environ.get("AzureWebJobsStorage__blobServiceUri")
                client_id = os.environ.get("AzureWebJobsStorage__clientId")
                if blob_service_uri:
                    logging.info(f"Using identity-based auth for blob storage: {blob_service_uri}")
                    if client_id:
                        # Use user-assigned managed identity with specific client ID
                        logging.info(f"Using user-assigned managed identity with client ID: {client_id}")
                        credential = ManagedIdentityCredential(client_id=client_id)
                    else:
                        # Use system-assigned managed identity or default credential
                        credential = DefaultAzureCredential()
                    self._blob_service_client = BlobServiceClient(blob_service_uri, credential=credential)
                else:
                    logging.warning("No storage configuration found")
        return self._blob_service_client
    
    def _load_all_occupations(self) -> None:
        """
        Load all occupation JSON files from blob storage into memory.
        This is called once and the data is reused for all subsequent requests.
        Supports up to 20 JSON files in the occupation container.
        """
        if self._is_loaded:
            logging.info("Occupation data already loaded, skipping reload")
            return
        
        with self._lock:
            # Double-check after acquiring lock
            if self._is_loaded:
                return
            
            logging.info("Loading all occupation data from blob storage...")
            blob_client = self._get_blob_client()
            
            if not blob_client:
                logging.warning("AzureWebJobsStorage connection string not configured")
                self._is_loaded = True
                return
            
            try:
                container_client = blob_client.get_container_client("occupation")
                
                # List and load all blobs (up to 20 files)
                file_count = 0
                for blob in container_client.list_blobs():
                    if file_count >= 20:
                        logging.warning("Maximum of 20 occupation files reached, stopping load")
                        break
                    
                    if blob.name.endswith(".json"):
                        try:
                            blob_client_item = container_client.get_blob_client(blob.name)
                            data = blob_client_item.download_blob().readall().decode("utf-8")
                            occupation_data = json.loads(data)
                            
                            # Store by occupation name (filename without .json extension)
                            occupation_name = blob.name[:-5]
                            self._occupation_data[occupation_name] = occupation_data
                            file_count += 1
                            logging.info(f"Loaded occupation: {occupation_name}")
                        except json.JSONDecodeError as e:
                            logging.warning(f"Failed to parse {blob.name}: {e}")
                        except Exception as e:
                            logging.warning(f"Failed to load {blob.name}: {e}")
                
                self._is_loaded = True
                logging.info(f"Successfully loaded {len(self._occupation_data)} occupation files")
                
            except Exception as e:
                logging.error(f"Error loading occupation data from blob storage: {e}")
                self._is_loaded = True  # Mark as loaded to prevent retry loops
    
    def get_occupation_data(self, occupation_name: str = None) -> str:
        """
        Get occupation data from the in-memory cache.
        If occupation_name is provided, returns data for that specific occupation.
        If occupation_name is None or empty, returns all occupation data.
        
        Args:
            occupation_name: The name of the occupation to retrieve, or None for all.
            
        Returns:
            JSON string containing the occupation data.
        """
        # Ensure data is loaded
        self._load_all_occupations()
        
        # If no occupation name provided, return all occupations
        if not occupation_name:
            logging.info("Returning all occupation data from cache")
            return json.dumps(list(self._occupation_data.values()))
        
        # Return specific occupation
        if occupation_name in self._occupation_data:
            logging.info(f"Cache HIT for occupation: {occupation_name}")
            return json.dumps(self._occupation_data[occupation_name])
        else:
            logging.warning(f"Occupation not found: {occupation_name}")
            available = list(self._occupation_data.keys())
            return json.dumps({
                "error": f"Occupation '{occupation_name}' not found",
                "available_occupations": available
            })
    
    def get_all_occupations(self) -> str:
        """
        Get a list of all available occupation names from the cache.
        
        Returns:
            JSON string containing the list of all occupation names.
        """
        # Ensure data is loaded
        self._load_all_occupations()
        
        occupation_names = list(self._occupation_data.keys())
        logging.info(f"Returning {len(occupation_names)} occupation names from cache")
        return json.dumps(occupation_names)
    
    def clear_cache(self) -> None:
        """Clear all cached data and force reload on next access."""
        with self._lock:
            self._occupation_data.clear()
            self._is_loaded = False
            logging.info("Occupation data cache cleared")
    
    def is_loaded(self) -> bool:
        """Check if occupation data has been loaded."""
        return self._is_loaded
    
    def get_occupation_count(self) -> int:
        """Get the number of loaded occupations."""
        return len(self._occupation_data)

# Global cache instance
occupation_cache = OccupationDataCache()

class ToolProperty:
    def __init__(self, property_name: str, property_type: str, description: str):
        self.propertyName = property_name
        self.propertyType = property_type
        self.description = description

    def to_dict(self):
        return {
            "propertyName": self.propertyName,
            "propertyType": self.propertyType,
            "description": self.description,
        }


# Define the tool properties using the ToolProperty class
tool_properties_save_snippets_object = [
    ToolProperty(_SNIPPET_NAME_PROPERTY_NAME, "string", "The name of the snippet."),
    ToolProperty(_SNIPPET_PROPERTY_NAME, "string", "The content of the snippet."),
]

tool_properties_get_snippets_object = [ToolProperty(_SNIPPET_NAME_PROPERTY_NAME, "string", "The name of the snippet.")]

# Convert the tool properties to JSON
tool_properties_save_snippets_json = json.dumps([prop.to_dict() for prop in tool_properties_save_snippets_object])
tool_properties_get_snippets_json = json.dumps([prop.to_dict() for prop in tool_properties_get_snippets_object])


# Tool properties for occupation/job-related functions
tool_properties_get_available_occupations_object = [
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_all_ranks_object = [
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_job_codes_for_rank_object = [
    ToolProperty(_RANK_CODE_PROPERTY_NAME, "string", "Rank code"),
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_tasks_for_job_code_object = [
    ToolProperty(_JOB_CODE_PROPERTY_NAME, "string", "Job code"),
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_tasks_for_duty_area_object = [
    ToolProperty("duty_area_code", "string", "Duty area code"),
    ToolProperty(_JOB_CODE_PROPERTY_NAME, "string", "Job code"),
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_skills_for_job_code_object = [
    ToolProperty(_JOB_CODE_PROPERTY_NAME, "string", "Job code"),
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_knowledge_for_job_code_object = [
    ToolProperty(_JOB_CODE_PROPERTY_NAME, "string", "Job code"),
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

tool_properties_get_all_duty_areas_object = [
    ToolProperty(_OCC_NAME_PROPERTY_NAME, "string", "Occupation Name"),
]

# Convert the tool properties to JSON
tool_properties_get_available_occupations_json = json.dumps([prop.to_dict() for prop in tool_properties_get_available_occupations_object])
tool_properties_get_all_ranks_json = json.dumps([prop.to_dict() for prop in tool_properties_get_all_ranks_object])
tool_properties_get_job_codes_for_rank_json = json.dumps([prop.to_dict() for prop in tool_properties_get_job_codes_for_rank_object])
tool_properties_get_tasks_for_job_code_json = json.dumps([prop.to_dict() for prop in tool_properties_get_tasks_for_job_code_object])
tool_properties_get_tasks_for_duty_area_json = json.dumps([prop.to_dict() for prop in tool_properties_get_tasks_for_duty_area_object])
tool_properties_get_skills_for_job_code_json = json.dumps([prop.to_dict() for prop in tool_properties_get_skills_for_job_code_object])
tool_properties_get_knowledge_for_job_code_json = json.dumps([prop.to_dict() for prop in tool_properties_get_knowledge_for_job_code_object])
tool_properties_get_all_duty_areas_json = json.dumps([prop.to_dict() for prop in tool_properties_get_all_duty_areas_object])
tool_properties_list_all_occupations_json = json.dumps([])


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="hello_mcp",
    description="Hello world.",
    toolProperties="[]",
)
def hello_mcp(context) -> None:
    """
    A simple function that returns a greeting message.

    Args:
        context: The trigger context (not used in this function).

    Returns:
        str: A greeting message.
    """
    return "Hello I am MCPTool!"


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_snippet",
    description="Retrieve a snippet by name.",
    toolProperties=tool_properties_get_snippets_json,
)
@app.generic_input_binding(arg_name="file", type="blob", connection="AzureWebJobsStorage", path=_BLOB_PATH)
def get_snippet(file: func.InputStream, context) -> str:
    """
    Retrieves a snippet by name from Azure Blob Storage.

    Args:
        file (func.InputStream): The input binding to read the snippet from Azure Blob Storage.
        context: The trigger context containing the input arguments.

    Returns:
        str: The content of the snippet or an error message.
    """
    snippet_content = file.read().decode("utf-8")
    logging.info(f"Retrieved snippet: {snippet_content}")
    return snippet_content


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="save_snippet",
    description="Save a snippet with a name.",
    toolProperties=tool_properties_save_snippets_json,
)
@app.generic_output_binding(arg_name="file", type="blob", connection="AzureWebJobsStorage", path=_BLOB_PATH)
def save_snippet(file: func.Out[str], context) -> str:
    content = json.loads(context)
    snippet_name_from_args = content["arguments"][_SNIPPET_NAME_PROPERTY_NAME]
    snippet_content_from_args = content["arguments"][_SNIPPET_PROPERTY_NAME]

    if not snippet_name_from_args:
        return "No snippet name provided"

    if not snippet_content_from_args:
        return "No snippet content provided"

    file.set(snippet_content_from_args)
    logging.info(f"Saved snippet: {snippet_content_from_args}")
    return f"Snippet '{snippet_content_from_args}' saved successfully"


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_available_occupations",
    description="Get a list of available occupations.",
    toolProperties=tool_properties_get_available_occupations_json,
)
def get_available_occupations(context) -> str:
    """
    Retrieves a list of available occupations using cached data.
    Data is loaded once and reused across all subsequent calls.
    If no occupation name is provided, returns all occupation data.

    Args:
        context: The trigger context containing the input arguments.

    Returns:
        str: A JSON string containing the list of available occupations.
    """
    content = json.loads(context)
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    # If no occupation name provided, load all occupations data
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    logging.info(f"Retrieved occupations for: {occupation_name if occupation_name else 'all'}")
    return occupations_content 
    
#Create a tool for list all occupations
@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="list_all_occupations",
    description="List all available occupations.",
    toolProperties=tool_properties_list_all_occupations_json,
)
def list_all_occupations(context) -> str:
    """
    Lists all available occupations.

    Args:
        context: The trigger context containing the input arguments.

    Returns:
        str: A JSON string containing the list of all available occupations.
    """
    occupations_content = occupation_cache.get_all_occupations()
    logging.info("Retrieved all occupations")
    return occupations_content

@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_all_ranks",
    description="Get a list of all unique ranks in the occupation data.",
    toolProperties=tool_properties_get_all_ranks_json,
)
def get_all_ranks(context) -> str:
    """
    Get a list of all unique ranks in the occupation data.
    
    Args:
        occupation: Optional occupation name to filter by. If not provided, returns ranks from all occupations.
    """
    content = json.loads(context)
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data for specific occupation or all occupations
    if occupation_name:
        # Single occupation - data is a list of records
        data_source = data if isinstance(data, list) else []
    else:
        # All occupations - data is a list of lists, flatten it
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    ranks = sorted(list(set(record.get('Rank', 'Unknown') for record in data_source if isinstance(record, dict))))
    
    result = {
        "occupation_filter": occupation_name or "All",
        "total_ranks": len(ranks),
        "ranks": ranks
    }
    return json.dumps(result, indent=2)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_job_codes_for_rank",
    description="Get job codes for a specific rank in an occupation.",
    toolProperties=tool_properties_get_job_codes_for_rank_json,
)
def get_job_codes_for_rank(context) -> str:
    """
    Get job codes for a specific rank in an occupation.
    
    Args:
        rank: The rank to filter by.
        occupation: Optional occupation name to filter by.
    """
    content = json.loads(context)
    rank = content["arguments"].get(_RANK_CODE_PROPERTY_NAME, "")
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    if not rank:
        return json.dumps({"error": "Rank is required"}, indent=2)
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data source
    if occupation_name:
        data_source = data if isinstance(data, list) else []
    else:
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    # Filter by rank and get unique job codes
    job_codes = {}
    for record in data_source:
        if isinstance(record, dict) and record.get('Rank') == rank:
            job_code = record.get('JobCode')
            if job_code and job_code not in job_codes:
                job_codes[job_code] = {
                    "job_code": job_code,
                    "occupation_requirement": record.get('OccupationRequirement', 'Unknown'),
                    "occupation": record.get('Occupation', 'Unknown')
                }
    
    result = {
        "rank_filter": rank,
        "occupation_filter": occupation_name or "All",
        "total_job_codes": len(job_codes),
        "job_codes": list(job_codes.values())
    }
    return json.dumps(result, indent=2)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_tasks_for_job_code",
    description="Get tasks for a specific job code in an occupation.",
    toolProperties=tool_properties_get_tasks_for_job_code_json,
)
def get_tasks_for_job_code(context) -> str:
    """
    Get tasks for a specific job code in an occupation.
    
    Args:
        job_code: The job code to filter by.
        occupation: Optional occupation name to filter by.
    """
    content = json.loads(context)
    job_code = content["arguments"].get(_JOB_CODE_PROPERTY_NAME, "")
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    if not job_code:
        return json.dumps({"error": "Job code is required"}, indent=2)
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data source
    if occupation_name:
        data_source = data if isinstance(data, list) else []
    else:
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    # Filter by job code and category = Task
    tasks = []
    for record in data_source:
        if isinstance(record, dict) and record.get('JobCode') == job_code and record.get('Category') == 'Task':
            tasks.append({
                "task_code": record.get('TaskCode'),
                "task_description": record.get('TaskDescription'),
                "duty_area_code": record.get('DutyAreaGroupCode'),
                "duty_area_name": record.get('DutyAreaGroupName'),
                "is_required": record.get('IsRequired', False)
            })
    
    result = {
        "job_code_filter": job_code,
        "occupation_filter": occupation_name or "All",
        "total_tasks": len(tasks),
        "tasks": tasks
    }
    return json.dumps(result, indent=2)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_tasks_for_duty_area",
    description="Get tasks for a specific duty area in an occupation.",
    toolProperties=tool_properties_get_tasks_for_duty_area_json,
)
def get_tasks_for_duty_area(context) -> str:
    """
    Get tasks for a specific duty area in an occupation.
    
    Args:
        duty_area_code: The duty area code to filter by.
        job_code: Optional job code to filter by.
        occupation: Optional occupation name to filter by.
    """
    content = json.loads(context)
    duty_area_code = content["arguments"].get("duty_area_code", "")
    job_code = content["arguments"].get(_JOB_CODE_PROPERTY_NAME, "")
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    if not duty_area_code:
        return json.dumps({"error": "Duty area code is required"}, indent=2)
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data source
    if occupation_name:
        data_source = data if isinstance(data, list) else []
    else:
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    # Filter by duty area code and optionally job code, category = Task
    tasks = []
    for record in data_source:
        if isinstance(record, dict) and record.get('DutyAreaGroupCode') == duty_area_code and record.get('Category') == 'Task':
            if job_code and record.get('JobCode') != job_code:
                continue
            tasks.append({
                "task_code": record.get('TaskCode'),
                "task_description": record.get('TaskDescription'),
                "job_code": record.get('JobCode'),
                "rank": record.get('Rank'),
                "is_required": record.get('IsRequired', False)
            })
    
    result = {
        "duty_area_code_filter": duty_area_code,
        "job_code_filter": job_code or "All",
        "occupation_filter": occupation_name or "All",
        "total_tasks": len(tasks),
        "tasks": tasks
    }
    return json.dumps(result, indent=2)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_skills_for_job_code",
    description="Get skills for a specific job code in an occupation.",
    toolProperties=tool_properties_get_skills_for_job_code_json,
)
def get_skills_for_job_code(context) -> str:
    """
    Get skills for a specific job code in an occupation.
    
    Args:
        job_code: The job code to filter by.
        occupation: Optional occupation name to filter by.
    """
    content = json.loads(context)
    job_code = content["arguments"].get(_JOB_CODE_PROPERTY_NAME, "")
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    if not job_code:
        return json.dumps({"error": "Job code is required"}, indent=2)
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data source
    if occupation_name:
        data_source = data if isinstance(data, list) else []
    else:
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    # Filter by job code and category = Skill
    skills = []
    for record in data_source:
        if isinstance(record, dict) and record.get('JobCode') == job_code and record.get('Category') == 'Skill':
            skills.append({
                "skill_code": record.get('TaskCode'),
                "skill_description": record.get('TaskDescription'),
                "duty_area_code": record.get('DutyAreaGroupCode'),
                "duty_area_name": record.get('DutyAreaGroupName'),
                "is_required": record.get('IsRequired', False)
            })
    
    result = {
        "job_code_filter": job_code,
        "occupation_filter": occupation_name or "All",
        "total_skills": len(skills),
        "skills": skills
    }
    return json.dumps(result, indent=2)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_knowledge_for_job_code",
    description="Get knowledge items for a specific job code in an occupation.",
    toolProperties=tool_properties_get_knowledge_for_job_code_json,
)
def get_knowledge_for_job_code(context) -> str:
    """
    Get knowledge items for a specific job code in an occupation.
    
    Args:
        job_code: The job code to filter by.
        occupation: Optional occupation name to filter by.
    """
    content = json.loads(context)
    job_code = content["arguments"].get(_JOB_CODE_PROPERTY_NAME, "")
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    if not job_code:
        return json.dumps({"error": "Job code is required"}, indent=2)
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data source
    if occupation_name:
        data_source = data if isinstance(data, list) else []
    else:
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    # Filter by job code and category = Knowledge
    knowledge_items = []
    for record in data_source:
        if isinstance(record, dict) and record.get('JobCode') == job_code and record.get('Category') == 'Knowledge':
            knowledge_items.append({
                "knowledge_code": record.get('TaskCode'),
                "knowledge_description": record.get('TaskDescription'),
                "duty_area_code": record.get('DutyAreaGroupCode'),
                "duty_area_name": record.get('DutyAreaGroupName'),
                "is_required": record.get('IsRequired', False)
            })
    
    result = {
        "job_code_filter": job_code,
        "occupation_filter": occupation_name or "All",
        "total_knowledge_items": len(knowledge_items),
        "knowledge_items": knowledge_items
    }
    return json.dumps(result, indent=2)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_all_duty_areas",
    description="Get all unique duty areas in an occupation.",
    toolProperties=tool_properties_get_all_duty_areas_json,
)
def get_all_duty_areas(context) -> str:
    """
    Get all unique duty areas in an occupation.
    
    Args:
        occupation: Optional occupation name to filter by.
    """
    content = json.loads(context)
    occupation_name = content["arguments"].get(_OCC_NAME_PROPERTY_NAME, "")
    
    # Get occupation data from cache
    occupations_content = occupation_cache.get_occupation_data(occupation_name if occupation_name else None)
    data = json.loads(occupations_content)
    
    # Check if we got an error response
    if isinstance(data, dict) and "error" in data:
        return json.dumps(data, indent=2)
    
    # Get data source
    if occupation_name:
        data_source = data if isinstance(data, list) else []
    else:
        data_source = []
        for occ_data in data:
            if isinstance(occ_data, list):
                data_source.extend(occ_data)
    
    # Get unique duty areas
    duty_areas = {}
    for record in data_source:
        if isinstance(record, dict):
            code = record.get('DutyAreaGroupCode')
            name = record.get('DutyAreaGroupName')
            if code and code not in duty_areas:
                duty_areas[code] = {
                    "code": code,
                    "name": name
                }
    
    # Sort by code
    sorted_duty_areas = sorted(duty_areas.values(), key=lambda x: x['code'])
    
    result = {
        "occupation_filter": occupation_name or "All",
        "total_duty_areas": len(sorted_duty_areas),
        "duty_areas": sorted_duty_areas
    }
    return json.dumps(result, indent=2)

#
