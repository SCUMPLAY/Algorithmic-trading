import binance
import asyncio
import time

websocket = None

# функция для запуска процесса вебсокета
def run(_symbols, _depth, _ask_volume, _main_queue):
    global symbols
    global depth
    global ask_volume
    global main_queue
    symbols = _symbols
    depth = _depth
    ask_volume = _ask_volume
    main_queue = _main_queue
    asyncio.run(main())

# основная функция вебсокета
async def main():
    global client
    global websocket
    # создаем клиент
    client = binance.Spot(asynced=True)
    # список для стримов
    streams = []
    for sym in symbols:
        # добавляем стримы в список
        streams.append(f"{sym.symbol.lower()}@depth{depth}")
    # подключаемся к вебсокету
    websocket = await client.websocket(streams, on_open=on_open, on_message=on_message,
                                       on_close=on_close, on_error=on_error)

# функция для обработки подключения вебсокета
async def on_open(ws):
    print(f"Подключение установлено")

# функция для обработки закрытия вебсокета
async def on_close(ws):
    pass

# функция для обработки ошибок вебсокета
async def on_error(ws, error):
    print(f"on_error: {error}")

# функция для обработки сообщений
async def on_message(ws, msg):
    # если сообщение содержит данные
    if 'data' in msg:
        # получаем символ из названия стрима (из btcusdt@depth20 получаем BTCUSDT)
        symbol = msg['stream'].split('@')[0].upper()
        # считаем объем всех ask'ов
        ask_vol = sum([float(ask[1]) for ask in msg['data']['asks']])
        # ищем bid, объем которого больше или равен объему ask'ов в ask_volume раз
        for num, bid in enumerate(msg['data']['bids']):
            if float(bid[1]) >= ask_vol * ask_volume:
                # получаем цену и объем
                bid_price, bid_volume = float(bid[0]), float(bid[1])
                # выводим информацию
                print(f"Обнаружена плотность по паре {symbol} цена {bid[0].rstrip('0')}, объем "
                      f"${round(bid_price * bid_volume, 2)}, который в "
                      f"{round(bid_volume / ask_vol, 2)} раз больше объема {depth} ask'ов")
                # добавляем информацию в очередь
                main_queue.put((symbol, bid_price, bid_volume, ask_vol, num))
                break


