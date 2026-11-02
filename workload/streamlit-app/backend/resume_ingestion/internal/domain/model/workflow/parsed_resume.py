from pydantic import BaseModel, ConfigDict, Field

class Experience(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, alias="title")
    company: str | None = Field(default=None, alias="company")
    start_date: str | None = Field(default=None, alias="start_date")
    end_date: str | None = Field(default=None, alias="end_date")
    responsibilities: list[str] = Field(default_factory=list, alias="responsibilities")


class Education(BaseModel):
    model_config = ConfigDict(extra="forbid")

    degree: str | None = Field(default=None, alias="degree")
    institution: str | None = Field(default=None, alias="institution")
    graduation_date: str | None = Field(default=None, alias="graduation_date")


class ParsedResume(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, alias="name")
    contact: dict[str, str] = Field(default_factory=dict, alias="contact")
    summary: str | None = Field(default=None, alias="summary")
    experience: list[Experience] = Field(default_factory=list, alias="experience")
    education: list[Education] = Field(default_factory=list, alias="education")
    skills: list[str] = Field(default_factory=list, alias="skills")
