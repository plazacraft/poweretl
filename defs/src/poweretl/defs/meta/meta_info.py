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
        object_id (int): Unique id of object in meta
        status (Status): Status of the operation execution
        operation (Operation): Operation to run
    """

    object_id: int = None
    status: Status = None
    operation: Operation = None
