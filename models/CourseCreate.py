from pydantic import BaseModel, Field
from datetime import date
from typing import Optional

class CourseCreate(BaseModel):
    CourseName: str = Field(..., example="Introduction to Python")
    University: str = Field(..., example="Harvard University")
    City: str = Field(..., example="Cambridge")
    Country: str = Field(..., example="USA")
    CourseDescription: Optional[str] = Field(None, example="A beginner's course in Python.")
    StartDate: date = Field(..., example="2023-09-01")
    EndDate: date = Field(..., example="2023-12-01")
    Price: float = Field(..., example=199.99)
    Currency: str = Field(..., example="USD")

    class Config:
        orm_mode = True
