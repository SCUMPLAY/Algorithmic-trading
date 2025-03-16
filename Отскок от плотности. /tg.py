import config
from aiogram import Bot, Dispatcher

# создаем бота
bot = Bot(token=config.TG_TOKEN, parse_mode='HTML')
# cоздаем диспетчер
dp = Dispatcher()


# функция для запуска бота
async def run():
    # пропускаем старые сообщения
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        # запускаем бота
        await dp.start_polling(bot)
    finally:
        # закрываем сессию после завершения работы
        await bot.session.close()