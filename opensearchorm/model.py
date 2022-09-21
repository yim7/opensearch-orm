from typing import ClassVar, Optional
from pydantic import BaseModel as RawBaseModel


class BaseModel(RawBaseModel):
    __index__: ClassVar[Optional[str]] = None

    @classmethod
    def default_fields(cls):
        return list(cls.__fields__.keys())
