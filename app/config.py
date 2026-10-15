from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    rabbitmq_url: str
    api_key: str

    # Seconds to wait before retry 1, 2 and 3 of a failed message.
    retry_delays: list[float] = [2, 4, 8]
    gateway_delay_min: float = 2
    gateway_delay_max: float = 5
    gateway_success_rate: float = 0.9
    webhook_timeout: float = 5
    outbox_poll_interval: float = 0.5


@lru_cache
def get_settings() -> Settings:
    return Settings()
