import config
from sqlalchemy import Column, Integer, String, Float, Boolean, select, BigInteger, desc
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# создаем базовый класс для работы с БД
Base = declarative_base()


# создаем таблицу с основными настройками
class Config(Base):
    __tablename__ = 'config'
    key = Column(String, primary_key=True)
    value = Column(String)


class Trades(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    order_size = Column(Float)
    status = Column(String)
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    entry_price = Column(Float)
    quantity = Column(Float)
    take_price = Column(Float)
    stop_price = Column(Float)
    profit = Column(Float)


# класс для хранения конфигации
class ConfigInfo:
    api_key: str
    api_secret: str
    trade_mode: int
    depth: int
    streams_count: int
    asset: str
    ask_volume: float
    order_size: float
    blacklist: list
    max_positions: int
    min_volume: float
    min_density_num: int
    entry_timeout: int
    take_profit: float

    def __init__(self, data):
        for key in self.__class__.__annotations__:
            setattr(self, key, None)
        # преобразование данных
        for key, value in data.items():
            try:
                # пытаемся преобразовать в int
                value = int(value)
            except:
                # если не получилось
                try:
                    # пытаемся преобразовать в float
                    value = float(value)
                except:
                    # если не получилось то пропускаем и он будет строкой
                    pass
            if key in ('blacklist', ):
                value = value.split(',')
            # присваиваем значение значению класса
            setattr(self, key, value)


# параметры по умолчанию
default_config = {
    'trade_mode': 0,
    'depth': 20,
    'streams_count': 50,
    'asset': 'USDT',
    'ask_volume': 10,
    'order_size': 10,
    'max_positions': 2,
    'min_volume': 5000,
    'blacklist': 'BTCUSDT',
    'min_density_num': 10,
    'entry_timeout': 30,
    'take_profit': 1
}

# функция для загрузки конфигурации
async def load_config():
    for key in ConfigInfo.__annotations__.keys():
        # перебираем все ключи и создаем их если их нет
        try:
            # пытаемся записать параметры по умолчанию в базу
            async with Session() as s:
                s.add(Config(key=key, value=str(default_config.get(key))))
                await s.commit()
        except:
            pass
    # создание сессии для работы с БД
    async with Session() as s:
        # загрузка конфигурации
        result = (await s.execute(select(Config))).scalars().all()
        # возвращаем результат
        return ConfigInfo({i.key: i.value for i in result})


# функция для получения сессии
async def get_session():
    # глобальная переменная сессии
    global Session
    # создаем асинхронный движок для работы с БД используя конфигурацию из config.py
    engine = create_async_engine(f"postgresql+asyncpg://{config.DB_USER}:{config.DB_PASSWORD}"
                                 f"@localhost/{config.DB_NAME}")
    async with engine.begin() as conn:
        # создаем таблицы
        await conn.run_sync(Base.metadata.create_all)
    # создаем сессию, expire_on_commit=False - чтобы она не уничтожалась после коммита
    Session = async_sessionmaker(engine, expire_on_commit=False)
    # возвращаем сессию
    return Session