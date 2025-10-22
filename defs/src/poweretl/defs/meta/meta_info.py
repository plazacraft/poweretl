from dataclasses import dataclass
from enum import Enum


class Operation(Enum):
    NEW = "new"
    UPDATED = "updated"
    DELETED = "deleted"


class Status(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class MetaInfo:
    """Represents metadata information for state of provisioning of target resource.
    Attributes:
        object_id (str): Globally unique id of object in meta
        status (str): Status of the operation execution
        operation (str): Operation to run
    """

    object_id: str = None
    # use str to avoid problems with serialization of Enums
    status: str = None
    # use str to avoid problems with serialization of Enums
    operation: str = None
