import asyncio
import aiogram
from aiogoogle import Aiogoogle, HTTPError
import logging
from datetime import datetime, timedelta
import os
import json
import boto3
from aiogram import Bot, Dispatcher, types
from enum import Enum


class State(Enum):
    DEFAULT = 0
    CITY_PICK = 1
    ROOM_PICK = 2
    DATE_PICK = 3
    TIME_PICK = 4


BOT_KEY = '<bot_key>'
bot = Bot(BOT_KEY)
log = logging.getLogger(__name__)
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO').upper())
dynamodb = boto3.resource('dynamodb')
bot_state = dynamodb.Table('bot_base')
quest_table = dynamodb.Table('quest_rooms')


async def start(message: types.Message):
    response = bot_state.get_item(Key={'chat_id': message.chat.id})
    if not response.get('Item', None):
        bot_state.put_item(Item={'chat_id': message.chat.id, 'chat_state': State.DEFAULT.value, 'a': 'a'})
    await bot.send_message(message.chat.id, 'Hello, {}!'.format(message.from_user.first_name))
    await help(message)
    

async def help(message: types.Message):
    await bot.send_message(message.chat.id, 'Some helpful commands:\n' +
                        '/help - help panel\n' +
                        '/add_reservation - add new reservation\n' +
                        '/check_reservations - check all reserved rooms\n' +
                        '/drop - stop reservation\n')


async def drop(message: types.Message):
    bot_state.update_item(
        Key={'chat_id': message.chat.id},
        UpdateExpression='set chat_state = :n',
        ExpressionAttributeValues={
            ':n': State.DEFAULT.value
        },
        ReturnValues='UPDATED_NEW'
    )
    remove_keyboard = types.ReplyKeyboardRemove()
    await bot.send_message(message.chat.id, "Reservation is stopped", reply_markup=remove_keyboard)
    
    
async def check_reservations(message: types.Message):
    result_set = quest_table.scan()
    reply = ''
    for room in result_set['Items']:
        for date in room.get('dates', {}):
            for time in room['dates'][date]:
                if room['dates'][date][time] == message.chat.id:
                    room_name = room['room_name']
                    city = room['city']
                    reply += f'{city}, {room_name}, {date}, {time}\n'
    remove_keyboard = types.ReplyKeyboardRemove()
    await bot.send_message(message.chat.id, reply, reply_markup=remove_keyboard)


async def add_reservation(message: types.Message):
    result_set = quest_table.scan()
    cities = list(set(map(lambda x: x['city'], result_set['Items'])))
    keyboard = types.ReplyKeyboardMarkup()
    keyboard.add(*cities)
    bot_state.update_item(
        Key={'chat_id': message.chat.id},
        UpdateExpression='set chat_state = :n',
        ExpressionAttributeValues={
            ':n': State.CITY_PICK.value
        },
        ReturnValues='UPDATED_NEW'
    )
    await bot.send_message(message.chat.id, "Pick your city", reply_markup=keyboard)


async def add_room(message: types.Message):
    result_set = quest_table.scan()
    cities = set(map(lambda x: x['city'], result_set['Items']))
    keyboard = types.ReplyKeyboardMarkup()
    picked_city = message.text
    if picked_city not in cities:
        keyboard.add(*cities)
        await bot.send_message(message.chat.id, 'Pick is wrong', reply_markup=keyboard)
        return
    rooms = set(map(lambda x: x['room_name'], filter(lambda el: el['city']==picked_city, result_set['Items'])))
    keyboard.add(*rooms)
    bot_state.update_item(
        Key={'chat_id': message.chat.id},
        UpdateExpression='set chat_state = :n, picked_city = :c',
        ExpressionAttributeValues={
            ':n': State.ROOM_PICK.value,
            ':c': picked_city
        },
        ReturnValues='UPDATED_NEW'
    )
    await bot.send_message(message.chat.id, "Pick your room", reply_markup=keyboard)


async def add_date(message: types.Message):
    result_set = quest_table.scan()
    user_data = bot_state.get_item(Key={'chat_id': message.chat.id})
    picked_city = user_data['Item']['picked_city']
    rooms = set(map(lambda x: x['room_name'], filter(lambda el: el['city']==picked_city, result_set['Items'])))
    keyboard = types.ReplyKeyboardMarkup()
    picked_room = message.text
    if picked_room not in rooms:
        keyboard.add(*rooms)
        await bot.send_message(message.chat.id, 'Pick is wrong', reply_markup=keyboard)
        return
    bot_state.update_item(
        Key={'chat_id': message.chat.id},
        UpdateExpression='set chat_state = :n, picked_room = :r',
        ExpressionAttributeValues={
            ':n': State.DATE_PICK.value,
            ':r': picked_room
        },
        ReturnValues='UPDATED_NEW'
    )
    remove_keyboard = types.ReplyKeyboardRemove()
    await bot.send_message(message.chat.id, "Pick your date in format DD-MM-YYYY (onnly this year)", reply_markup=remove_keyboard)


async def add_time(message: types.Message):
    remove_keyboard = types.ReplyKeyboardRemove()
    try:
        day, month, year = map(int, message.text.split('-'))
        if datetime(year, month, day) + timedelta(days = 1) < datetime.now() or \
           datetime(year, month, day) > datetime(datetime.today().year + 1, 1, 1):
               await bot.send_message(message.chat.id, "Wrong date", reply_markup=remove_keyboard)
               return
    except ValueError:
        await bot.send_message(message.chat.id, "Wrong date", reply_markup=remove_keyboard)
        return
    user_data = bot_state.get_item(Key={'chat_id': message.chat.id})
    room_data = quest_table.get_item(Key={'room_name': user_data['Item']['picked_room']})
    dates = room_data.get('dates', {})
    times = set(dates.get(message.text, {}).keys())
    options = list(filter(lambda x: x not in times, (f'{i}:00' for i in range(9, 21))))
    if not options:
        await bot.send_message(message.chat.id, "All times are reserved. Pick another date, or tap \drop", reply_markup=remove_keyboard)
        return
    keyboard = types.ReplyKeyboardMarkup()
    keyboard.add(*options)
    bot_state.update_item(
        Key={'chat_id': message.chat.id},
        UpdateExpression='set chat_state = :n, picked_date = :c',
        ExpressionAttributeValues={
            ':n': State.TIME_PICK.value,
            ':c': message.text
        },
        ReturnValues='UPDATED_NEW'
    )
    await bot.send_message(message.chat.id, "Pick your time", reply_markup=keyboard)


async def confirm_pick(message: types.Message):
    user_data = bot_state.get_item(Key={'chat_id': message.chat.id})
    room_data = quest_table.get_item(Key={'room_name': user_data['Item']['picked_room']})['Item']
    dates = room_data.get('dates', {})
    times = set(dates.get(message.text, {}).keys())
    options = list(filter(lambda x: x not in times, (f'{i}:00' for i in range(9, 21))))
    if message.text not in options:
        keyboard = types.ReplyKeyboardMarkup()
        keyboard.add(*options)
        await bot.send_message(message.chat.id, "Wrong time pick", reply_markup=keyboard)
        return
    time = message.text
    room = user_data['Item']['picked_room']
    date = user_data['Item']['picked_date']
    bot_state.update_item(
        Key={'chat_id': message.chat.id},
        UpdateExpression='set chat_state = :n',
        ExpressionAttributeValues={
            ':n': State.DEFAULT.value
        },
        ReturnValues='UPDATED_NEW'
    )
    if not room_data.get('dates', None):
        room_data['dates'] = {}
    if not room_data['dates'].get(date, None):
        room_data['dates'][date] = {}
    room_data['dates'][date][time] = message.chat.id
    print(room_data['dates'])
    quest_table.update_item(
        Key={'room_name': room},
        UpdateExpression='set dates = :n',
        ExpressionAttributeValues={
            ':n': room_data['dates']
        },
        ReturnValues='UPDATED_NEW'
    )
    remove_keyboard = types.ReplyKeyboardRemove()
    await bot.send_message(message.chat.id, "Reservations was added", reply_markup=remove_keyboard)


async def process_message(message: types.Message):
    response = bot_state.get_item(Key={'chat_id': message.chat.id})
    state_value = response['Item']['chat_state']
    if State(state_value) == State.CITY_PICK:
        await add_room(message)
    if State(state_value) == State.ROOM_PICK:
        await add_date(message)
    if State(state_value) == State.DATE_PICK:
        await add_time(message)
    if State(state_value) == State.TIME_PICK:
        await confirm_pick(message)

# AWS Lambda funcs
async def register_handlers(dp: Dispatcher):
    """Registration all handlers before processing update."""
    dp.register_message_handler(start, commands=['start'])
    dp.register_message_handler(help, commands=['help'])
    dp.register_message_handler(drop, commands=['drop'])
    dp.register_message_handler(add_reservation, commands=['add_reservation'])
    dp.register_message_handler(check_reservations, commands=['check_reservations'])
    dp.register_message_handler(process_message)

    log.debug('Handlers are registered.')


async def process_event(event, dp: Dispatcher):
    """
    Converting an AWS Lambda event to an update and handling that
    update.
    """

    log.debug('Update: ' + str(event))

    Bot.set_current(dp.bot)
    update = types.Update.to_object(event)
    await dp.process_update(update)


async def main(event):
    """
    Asynchronous wrapper for initializing the bot and dispatcher,
    and launching subsequent functions.
    """

    # Bot and dispatcher initialization
    dp = Dispatcher(bot)

    await register_handlers(dp)
    await process_event(event, dp)

    return 'ok'


def lambda_handler(event, context):
    """AWS Lambda handler."""
    
    event_body = json.loads(event['body'])
    return asyncio.get_event_loop().run_until_complete(main(event_body))

