from dataclasses import dataclass, field
from typing import List, Optional, Dict


@dataclass
class Load:
    source: Optional[str] = None
    method: Optional[str] = None
    pathFiles: Optional[str] = None
    queryLoad: Optional[str] = None
    format: Optional[str] = None
    tempView: Optional[str] = None
    options: Optional[Dict] = field(default_factory=dict)
    pathQuery: Optional[str] = None
    bucket: Optional[str] = None
    createTableLocation: Optional[str] = None
    table: Optional[str] = None
    bucketConfig: Optional[str] = None


@dataclass
class Transform:
    sourceQuery: Optional[str] = None
    tempView: Optional[str] = None
    pathQuery: Optional[str] = None
    bucket: Optional[str] = None
    output: Optional[str] = None

@dataclass
class Delete:
    source: Optional[str] = None
    format: Optional[str] = None
    bucket: Optional[str] = None
    pathFilesToDelete: Optional[str] = None
    fieldToFilter: Optional[str] = None
    methodGetValues: Optional[str] = None
    listValues: Optional[List[str]] = None
    queryValues: Optional[str] = None
   

@dataclass
class Output:
    source: Optional[str] = None
    pathOutputFiles: Optional[str] = None
    mode: Optional[str] = None
    format: Optional[str] = None
    options: Optional[Dict] = field(default_factory=dict)
    tempViewToWrite: Optional[str] = None
    partitionedBy: Optional[List[str]] = field(default_factory=list)
    bucket: Optional[str] = None


@dataclass
class Configuration:
    load: List[Load] = field(default_factory=list)
    transform: List[Transform] = field(default_factory=list)
    delete: List[Delete] = field(default_factory=list)
    output: List[Output] = field(default_factory=list)
