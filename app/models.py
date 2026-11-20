"""请求 / 响应模型。"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field, field_validator

JOB_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"


class SegmentSpec(BaseModel):
    """一路输入段：path 相对挂载目录；order 决定同键的路序。"""

    path: str = Field(..., min_length=1, description="相对挂载目录的 JSONL 段文件路径")
    order: int = Field(..., ge=0, description="路序，从 0 开始；同键按路序升序输出")


class JobCreate(BaseModel):
    """登记作业（POST /jobs）。"""

    job_id: str = Field(..., pattern=JOB_ID_PATTERN)
    key_field: str = Field(..., min_length=1, description="记录中整数排序键的字段名")
    page_size: int = Field(..., gt=0, description="每页行数；只有末页允许短于该值")
    segments: list[SegmentSpec] = Field(..., min_length=1)

    @field_validator("segments")
    @classmethod
    def _check_orders(cls, v: list[SegmentSpec]) -> list[SegmentSpec]:
        orders = [s.order for s in v]
        if len(set(orders)) != len(orders):
            raise ValueError("segment order 必须唯一")
        if sorted(orders) != list(range(len(orders))):
            raise ValueError("segment order 必须是从 0 开始的连续整数")
        paths = [s.path for s in v]
        if len(set(paths)) != len(paths):
            raise ValueError("segment path 不能重复")
        for s in v:
            p = s.path
            if p.startswith(("/", "\\")) or ".." in Path(p).parts:
                raise ValueError(f"非法段路径: {p}")
        return v


class FileIdentity(BaseModel):
    """登记时刻捕获的段文件身份，回收时据此确认当前文件仍是同一批。

    上游在同路径放下一批段时，仅靠路径无法区分新旧文件；
    ``(dev, ino, size, mtime_ns)`` 标识文件实例，``sha256`` 再兜底
    内容（两份字节完全相同的文件也能靠 stat 身份区分）。
    """

    dev: int = Field(..., description="stat.st_dev：所在设备号")
    ino: int = Field(..., description="stat.st_ino：inode 号")
    size: int = Field(..., description="登记时文件字节数")
    mtime_ns: int = Field(..., description="登记时纳秒级修改时间")
    sha256: str = Field(..., description="登记时整文件内容的 SHA-256")


class SegmentState(BaseModel):
    path: str
    order: int
    offset: int = Field(0, description="下一条待处理记录在段文件中的字节偏移")
    exhausted: bool = False
    reclaimed: bool = False
    file_id: FileIdentity | None = Field(
        None, description="登记时捕获的段文件身份；旧版状态无此值（None）"
    )
    reclaim_deferred: bool = Field(
        False,
        description="已耗尽但同路径出现非本批文件（或旧版状态无法确认身份），"
        "放弃删除并保留文件，等待人工处理",
    )


class JobStatus(BaseModel):
    job_id: str
    key_field: str
    page_size: int
    segments: list[SegmentState]
    published_pages: int = Field(..., description="已发布页数（page_no 从 1 起）")
    total_published_rows: int
    finished: bool
    next_page_no: int
    page_counts: list[int] = Field(..., description="每页实际行数")
    warnings: list[str] = Field(
        default_factory=list,
        description="保留未删段等需要人工介入的告警（含段路径），去重持久化",
    )


class AdvanceResult(BaseModel):
    page_no: int | None = Field(None, description="本次新发布的页号；无新页（已完成）时为 null")
    page_size: int
    rows: int = Field(..., description="本次发布的行数")
    finished: bool
    reclaimed: list[str] = Field(default_factory=list, description="本次实际删除（回收）的段文件相对路径")
    deferred: list[str] = Field(
        default_factory=list,
        description="本次判定保留、未删除的段文件相对路径（同路径为非本批文件或身份无法确认）",
    )
    warnings: list[str] = Field(default_factory=list, description="本次产生或已有的人工介入告警")
    status: JobStatus


class RunResult(BaseModel):
    published: list[int] = Field(..., description="本次 run 新发布的页号列表")
    finished: bool
    reclaimed: list[str] = Field(default_factory=list, description="本次 run 实际删除的段文件相对路径")
    deferred: list[str] = Field(default_factory=list, description="本次 run 保留未删的段文件相对路径")
    warnings: list[str] = Field(default_factory=list, description="作业当前的人工介入告警")
    status: JobStatus


class PageMeta(BaseModel):
    page_no: int
    rows: int
    filename: str
