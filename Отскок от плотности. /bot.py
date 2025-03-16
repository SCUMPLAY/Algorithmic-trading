import config
import binance
import db
import tg
import ob
import asyncio
import multiprocessing
import traceback
import utils
import binance.error

# словарь для хранения символов
all_symbols: dict[str, binance.symbols.SymbolSpot] = {}
# список для процессов
processes = []
# очередь для передачи данных плотностей из процессов вебсокетов
main_queue = multiprocessing.Queue()
# словарь для хранения информации и последней плотности (для автоматической отмены ордера при уходе из плотности)
symbols_densities = {}
# переменная для хранения баланса
balance = .0
# переменная для хранения цены BNB
bnb_price = .0


# класс для хранения позиции
class Position:
    symbol: str
    quantity: float
    entry_price: float
    order_id: int
    status: bool
    commission: float
    qty: float
    coin: str
    density: float

    def __init__(self, symbol, quantity, entry_price, density):
        self.symbol = symbol
        self.quantity = quantity
        self.entry_price = entry_price
        self.order_id = None
        self.status = False
        self.commission = 0
        self.qty = None
        self.density = density
        # получаем базовый актив торговой пары (например BTC для BTCUSDT)
        for sym in all_symbols.values():
            if sym.symbol == symbol:
                self.coin = sym.base_asset
        else:
            self.coin = ''


# словарь для хранения позиций
positions: dict[str, Position] = {}


# функция запуска бота
async def main():
    # глобальные переменные
    global client
    global all_symbols
    global conf
    global session
    # создание сессии
    session = await db.get_session()
    # загрузка конфигурации
    conf = await db.load_config()
    # создаем клиент
    client = binance.Spot(conf.api_key, conf.api_secret, asynced=True)
    # загружаем все символы
    all_symbols = await client.load_symbols()
    # загружаем баланс
    asyncio.create_task(load_balance())
    # запускаем функцию получения плотностей от вебсокетов
    asyncio.create_task(get_density())
    # запускаем функцию проверки позиции
    asyncio.create_task(check_positions())
    # вебсокет для получения цены BNB
    await client.websocket(f"bnb{conf.asset.lower()}@miniTicker", on_message=bnb_ws, on_error=on_error)
    # запускаем вебсокет userdata
    await client.websocket_userdata(on_open=on_open, on_message=msg_userdata, on_error=on_error)
    # запускаем процессы вебсокетов
    await connect_websockets()
    # запускаем телеграмм бота
    await tg.run()


# функция для получения цены BNB
async def bnb_ws(ws, msg):
    global bnb_price
    bnb_price = float(msg['c'])


# функция для получения баланса при запуске бота
async def load_balance():
    global balance
    # получаем балансы
    account = await client.account()
    # пребираем активы
    for coin in account['balances']:
        # если нужный нам актив
        if coin['asset'] == conf.asset:
            # записываем баланс
            balance = float(coin['free'])


# функция для запуска процессов вебсокетов
async def connect_websockets():
    # глобальная переменная процессов
    global processes
    symbols = []
    # получаем список активных символов, которые торгуются на споте и имею нужный нам asset
    for sym in all_symbols.values():
        if sym.status == 'TRADING' and sym.quote_asset == conf.asset and 'SPOT' in sym.permissions:
            symbols.append(sym)
    # разбиваем список на подсписки
    symbols_lists = [symbols[i:i+conf.streams_count] for i in range(0, len(symbols), conf.streams_count)]
    # перебираем подсписки
    for s_list in symbols_lists:
        # создаем процесс
        proc = multiprocessing.Process(target=ob.run, args=(s_list, conf.depth, conf.ask_volume, main_queue))
        # добавляем процесс в список
        processes.append(proc)
        # запускаем процесс
        proc.start()


# функция для получения плотности из вебсокетов
async def get_density():
    # получаем объект loop (для работы с асинхронными задачами)
    loop = asyncio.get_event_loop()
    # вечный цикл
    while True:
        try:
            # получаем плотность используя run_in_executor (чтобы не блокировать поток выполнения, поскольку
            # метод multiprocessing.Queue.get() является синхронным)
            density = await loop.run_in_executor(None, main_queue.get)
            # передаем плотность в функцию new_density
            await new_density(*density)
        except:
            # ошибки пропускаем
            pass


# функция для обработки плотности
async def new_density(symbol, bid_price, bid_volume, ask_volume, num):
    global positions
    try:
        # записываем плотность в словарь (для отслеживания необходимости отмены лимиток, если цена ушла далеко от них)
        symbols_densities[symbol] = (bid_price, utils.get_ts())
        # если плотность находится дальше чем min_density_num, то пока еще рано ставить лимитку на вход
        if num > conf.min_density_num:
            return
        if balance < conf.order_size * config.MIN_NOTIONAL:
            print(f"{symbol}: {balance} < {conf.order_size} недостаточно средств")
            return
        # если позиция по этой паре уже открыта, то пропускаем
        if symbol in positions:
            print(f"{symbol} {bid_price} уже открыта")
            return
        # если пара в черном списке, то пропускаем
        if symbol in conf.blacklist:
            print(f"{symbol} {bid_price} в черном списке")
            return
        # если объем меньше min_volume, то пропускаем
        if bid_volume * bid_price < conf.min_volume:
            print(f"{symbol} {bid_volume * bid_price} слишком маленький объем")
            return
        # если уже открыто слишком много позиций, то пропускаем
        if len(positions) >= conf.max_positions:
            print(f"{symbol} {bid_price} уже открыто слишком много позиций")
            return
        # вносим позицию в словарь, чтобы у нас не открылась позиция несколько раз на следующем тике
        positions[symbol] = True
        # запускаем задачу открытия позиции
        asyncio.create_task(open_position(symbol, bid_price))
    except:
        # выводим ошибку
        print(traceback.format_exc())


# функция для открытия позиции
async def open_position(symbol, bid_price):
    global positions
    print(f"Открытие позиции по {symbol} цена {bid_price}")
    try:
        # получаем информацию о паре
        sym_info = all_symbols[symbol]
        # расчитываем цену входа (нам нужно выставить цену лимитки большую на 1 шаг цены)
        entry_price = round(bid_price + 10 ** -sym_info.tick_size, sym_info.tick_size)
        # расчитываем объем входа на основе order_size
        quantity = utils.round_down(conf.order_size / entry_price, sym_info.step_size)
        # если цена входа меньше min_notional, то увеличиваем объем на min_notional с запасом
        if quantity * entry_price < sym_info.min_notional * config.MIN_NOTIONAL:
            quantity = utils.round_up(sym_info.min_notional * config.MIN_NOTIONAL / entry_price, sym_info.step_size)
        # записываем позицию в словарь
        positions[symbol] = pos = Position(symbol, quantity, entry_price, bid_price)
        # выставляем лимитку
        order = await client.new_order(symbol=symbol, side='BUY', type='LIMIT_MAKER', price=entry_price,
                                       quantity=quantity)
        # записываем id ордера
        pos.order_id = order['orderId']
        print(f"Выставил лимитку по {symbol} цена {entry_price} объем {quantity}")
    except:
        # если не удалось открыть позицию, то удаляем позицию
        if symbol in positions:
            del positions[symbol]
        print(f"Не удалось открыть позицию по {symbol} цена {bid_price}\n{traceback.format_exc()}")
        return


async def on_open(ws):
    print("Вебсокет UserData подключен")


async def on_error(ws, error):
    print(f"Ошибка вебсокета: {error}")


# функция для обработки сообщений вебсокета UserData
async def msg_userdata(ws, msg):
    global positions
    global balance
    # определяем тип сообщения
    match msg['e']:
        # исполнение ордера
        case 'executionReport':
            if msg['X'] in ('FILLED', 'PARTIALLY_FILLED'):
                # получаем позицию
                if pos := positions.get(msg['s']):
                    # устанавливаем статус позиции
                    pos.status = True
                    # отпределяем в чем начислена комиссия
                    match msg['N']:
                        case 'BNB':
                            # если в BNB, то умножаем на цену
                            pos.commission += float(msg['n']) * bnb_price
                        case conf.asset:
                            # если в asset, то просто прибавляем
                            pos.commission += float(msg['n'])
                        case pos.coin:
                            # если в монете позиции, то умножаем на цену
                            pos.commission += float(msg['n']) * float(msg['p'])
                    # если ордер исполнен полностью
                    if msg['X'] == 'FILLED':
                        # если исполнился ордер на вход в позицию
                        if msg['S'] == 'BUY' and msg['o'] == 'LIMIT_MAKER':
                            # записываем сумму покупки
                            pos.qty = float(msg['Z'])
                            # создаем задачу выставления тейка и стопа
                            asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity))
                        # если исполнился ордер на выход из позиции
                        elif msg['S'] == 'SELL':
                            # закрываем позицию
                            asyncio.create_task(trade_closed(msg, pos))
        # изменение баланса
        case 'outboundAccountPosition':
            # пребираем активы
            for coin in msg['B']:
                # если актив совпадает с нашим asset'ом
                if coin['a'] == conf.asset:
                    # обновляем баланс
                    balance = float(coin['f'])


# функция для выставления тейка и стопа
async def set_stop_take(symbol, entry_price, quantity):
    global positions
    # получаем информацию о паре
    sym_info = all_symbols[symbol]
    # расчитываем цену тейка
    take_price = utils.round_up(entry_price * (1 + conf.take_profit / 100), sym_info.tick_size)
    # расчитываем цену стопа
    stop_price = round(entry_price - 10 ** -sym_info.tick_size * 2, sym_info.tick_size)
    # расчитываем цену стопа2 (лимитная цена ниже тригерной, чтобы стоп сработал по рынку)
    stop_price2 = utils.round_up(stop_price * (1 - config.STOP_PERCENT / 100), sym_info.tick_size)
    # создаем сессию
    async with session() as s:
        # записываем сделку
        trade = db.Trades(symbol=symbol, order_size=quantity, status='OPEN', open_time=utils.get_ts(),
                          entry_price=entry_price, quantity=quantity, take_price=take_price, stop_price=stop_price)
        # добавляем сделку
        s.add(trade)
        # сохраняем изменения в БД
        await s.commit()
    # формируем сообщение
    text = (f"Открыл сделку на <b>{quantity}</b> #{symbol}\n"
          f"Вход: <b>{entry_price}</b>\n"
          f"Стоп/Тейк: <b>{stop_price} / {take_price}</b>")
    # отправляем сообщение в телеграм канал
    await tg.bot.send_message(config.TG_CHANNEL, text)
    try:
        # выставляем тейк и стоп, используя OCO ордер
        orders = await client.new_oco_order(symbol=symbol, side='SELL', quantity=quantity, price=take_price,
                                        stopPrice=stop_price, stopLimitPrice=stop_price2, stopLimitTimeInForce='GTC')
    except:
        print(f"Ошибка при размещение тейка и стопа по {symbol}, закрываю позицию")
        # если ошибка, то закрываем позицию по рынку
        await client.new_order(symbol=symbol, side='SELL', quantity=quantity, type='MARKET')
        # удаляем позицию
        if symbol in positions:
            del positions[symbol]
        return
    print(f"Ордера для тейка и стопа установлены: {orders}")


async def trade_closed(msg, pos):
    async with session() as s:
        trade = await s.execute(db.select(db.Trades).where(db.Trades.symbol == pos.symbol).where(
            db.Trades.status == 'OPEN').order_by(db.desc(db.Trades.id)))
        trade = trade.scalars().all()
    if trade:
        trade = trade[0]
        trade.close_time = utils.get_ts()
        trade.profit = float(msg['Z']) - pos.qty - pos.commission
        match msg['o']:
            case 'LIMIT_MAKER':
                close_type = 'тейку'
                trade.status = 'CLOSED_TAKE'
            case 'STOP_LOSS_LIMIT':
                close_type = 'стопу'
                trade.status = 'CLOSED_STOP'
            case _:
                close_type = 'рынку'
                trade.status = 'CLOSED_MARKET'
        async with session() as s:
            s.add(trade)
            await s.commit()
        if trade.symbol in positions:
            del positions[trade.symbol]
        text = (f"Закрыл сделку на <b>{trade.quantity}</b> #{trade.symbol} по {close_type}\n"
                f"{'Прибыль' if trade.profit > 0 else 'Убыток'}: <b>{round(abs(trade.profit), 4)}</b>\n"
                f"Комиссия: <b>{round(abs(pos.commission), 4)} {conf.asset}</b>")
        await tg.bot.send_message(config.TG_CHANNEL, text)


# функция для проверки позиции
async def check_positions():
    global positions
    global symbols_densities
    # вечный цикл
    while True:
        # перебираем позиции
        for sym, pos in positions.items():
            # если по позиции уже выставлена лимитка (проверяем соответствие позиции классу Position, если мы еще не
            # успели выставить лимитку на вход, тогда класс будет еще bool (объект True), и его нам проверять рано)
            if isinstance(pos, Position):
                # если статус позиции новый, то получаем последнюю плотность по этой паре
                if not pos.status and (density := symbols_densities.get(pos.symbol)):
                    # если плотность совпадает с позицией и она ушла из зоны видимости более entry_timeout секунд назад
                    if pos.density != density[0] or utils.get_ts() - density[1] > conf.entry_timeout * 1000:
                        print(f"Отменяем лимитку на вход {pos.symbol} цена {pos.entry_price}")
                        # отменяем лимитку
                        asyncio.create_task(cancel_limit(pos.symbol, pos.order_id))
        # ждем CHECK_POSITION_TIMEOUT секунд
        await asyncio.sleep(config.CHECK_POSITION_TIMEOUT)


# функция для отмены позиции
async def cancel_limit(symbol, order_id):
    global positions
    try:
        # отменяем ордер
        await client.cancel_order(symbol=symbol, orderId=order_id)
    except:
        pass
    # удаляем позицию, если есть
    if symbol in positions:
        del positions[symbol]


if __name__ == '__main__':
    asyncio.run(main())