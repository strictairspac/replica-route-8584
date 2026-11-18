from typing import Literal
from pydantic import BaseModel, Field, model_validator

SUB_EVENT_KINDS = ('write', 'replicate', 'node_down', 'node_up',
                   'subscribe', 'sub_down', 'sub_up', 'ack')


class SubEvent(BaseModel):
    at_ms: int = Field(ge=0, le=86400000)
    kind: Literal['write', 'replicate', 'node_down', 'node_up',
                  'subscribe', 'sub_down', 'sub_up', 'ack']
    key: str | None = Field(default=None, max_length=120)
    value: str | None = Field(default=None, max_length=500)
    node: str | None = Field(default=None, max_length=80)
    subscriber: str | None = Field(default=None, max_length=80)
    seq: int | None = Field(default=None, ge=1, le=1000000000)
    position: int | None = Field(default=None, ge=0, le=1000000000)
    delay_ms: int | None = Field(default=None, ge=0, le=3600000)
    lost: bool = False
    out_of_order: bool = False
    from_position_seq: int | None = Field(default=None, ge=0)

    @model_validator(mode='after')
    def valid_event(self):
        if self.kind in ('replicate', 'node_down', 'node_up') and not self.node:
            raise ValueError(self.kind + ' 事件需要 node')
        if self.kind == 'replicate' and (self.position is None or self.delay_ms is None):
            raise ValueError('replicate 需要 position 和 delay_ms')
        if self.kind in ('subscribe', 'sub_down', 'sub_up', 'ack') and not self.subscriber:
            raise ValueError(self.kind + ' 事件需要 subscriber')
        if self.kind == 'ack' and self.seq is None:
            raise ValueError('ack 事件需要 seq')
        return self


class SubScenarioInput(BaseModel):
    code: str = Field(min_length=2, max_length=80)
    title: str = Field(min_length=2, max_length=160)
    events: list[SubEvent] = Field(min_length=1, max_length=100)
    note: str = Field(default='', max_length=300)

    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms > b.at_ms for a, b in zip(self.events, self.events[1:])):
            raise ValueError('事件必须按 at_ms 非递减排列')
        return self


class SubTrialInput(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    scenario_id: int = Field(gt=0)
    ack_policy: Literal['resend', 'reconcile'] = 'resend'
    delivery_ms: int = Field(default=120, ge=0, le=3600000)
    ack_timeout_ms: int = Field(default=400, ge=0, le=3600000)
    reconcile_rtt_ms: int = Field(default=300, ge=0, le=3600000)
    inflight_window: int = Field(default=1, ge=1, le=64)
