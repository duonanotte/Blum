from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str

    USE_RANDOM_DELAY_IN_RUN: bool = False
    RANDOM_DELAY_IN_RUN: list[int] = [5, 49930]

    TASKS: bool = False
    PLAY_GAMES: bool = False
    POINTS: list[int] = [190, 230]

    USE_REF: bool = False
    REF_ID: str = 'ref_QmiirCtfhH'

    SLEEP_TIME: list[int] = [28000, 41000]


settings = Settings()


