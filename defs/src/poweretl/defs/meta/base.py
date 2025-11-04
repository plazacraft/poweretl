from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from poweretl.defs.model import BaseCollection as mBaseCollection
from poweretl.defs.model import BaseItem as mBaseItem


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

    error_msg: Optional[str] = None

    updated_fields: dict[str, str] = field(default_factory=dict)

    model_last_update: Optional[str] = None
    meta_last_update: Optional[str] = None


@dataclass
class BaseItem(mBaseItem):
    """Base class for metadata objects.
    Attributes:
        meta (MetaInfo): Metadata information.
    """

    meta: MetaInfo = field(default_factory=MetaInfo)


@dataclass
class BaseCollection(mBaseCollection):

    items: dict[str, BaseItem] = field(
        default_factory=dict, metadata={"exclude_from_upgrader": True}
    )
