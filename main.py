import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')
dp = Dispatcher()


async def aggregate_data(dt_from, dt_upto, group_type):
    client = AsyncIOMotorClient('mongodb://mongodb:27017/')
    collection = client['sampleDB']['sample_collection']
    if group_type == 'hour':
        date_format = '%Y-%m-%dT%H:00:00'
    elif group_type == 'day':
        date_format = '%Y-%m-%dT00:00:00'
    elif group_type == 'month':
        date_format = '%Y-%m-01T00:00:00'
    else:
        raise ValueError
    pipeline = [
        {'$match': {'dt': {
            '$gte': datetime.fromisoformat(dt_from),
            '$lte': datetime.fromisoformat(dt_upto)
        }}},
        {'$group': {
            '_id': {'$dateToString': {
                'format': date_format, 'date': {'$toDate': '$dt'}
            }},
            'total': {'$sum': '$value'}
        }},
        {'$sort': {'_id': 1}}
    ]
    result = await collection.aggregate(pipeline).to_list(length=None)
    dataset = []
    labels = []
    dt_print = datetime.fromisoformat(dt_from)
    doc_id = 0
    while dt_print <= datetime.fromisoformat(dt_upto):
        if (
            doc_id < len(result)
            and result[doc_id]['_id'] == dt_print.strftime(date_format)
        ):
            dataset.append(result[doc_id]['total'])
            doc_id += 1
        else:
            dataset.append(0)
        labels.append(dt_print.strftime(date_format))
        if group_type == 'hour':
            dt_print += timedelta(hours=1)
        elif group_type == 'day':
            dt_print += timedelta(days=1)
        elif group_type == 'month':
            dt_print += timedelta(days=31)
            dt_print.replace(day=1)
    return json.dumps({'dataset': dataset, 'labels': labels})


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f'Hello, {message.from_user.full_name}!')


@dp.message()
async def echo_handler(message: Message) -> None:
    try:
        text = json.loads(message.text)
        await message.answer(
            await aggregate_data(
                text['dt_from'], text['dt_upto'], text['group_type']
            )
        )
    except json.JSONDecodeError:
        await message.answer(
            'Ожидалось сообщение, содержащее JSON с входными данными'
        )
    except KeyError:
        await message.answer('Ожидались ключи dt_from, dt_upto, group_type')
    except ValueError:
        await message.answer(
            'Формат dt_from/dt_upto должен быть:\n%Y-%m-%dT%H:%M:%S\n'
            'Пример: 2022-09-01T00:00:00\n\n'
            'group_type должен содержать hour, day или month'
        )
    except Exception as e:
        await message.answer(f'Произошла ошибка: {e}')


async def main() -> None:
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO, stream=sys.stdout
    )
    asyncio.run(main())
