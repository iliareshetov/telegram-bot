import logging

import pytz
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
import datetime
import calendar

from processVoiceMsg.dbservice import insert_booking, fetch_all_bookings_for_user

tz = pytz.timezone('Europe/Helsinki')


def get_users_bookings(update, context):
    data = fetch_all_bookings_for_user(update.effective_chat.id)
    processed_data = ''
    for row in data:
        processed_data += f'you have active booking on the date: {row[0].strftime("%d.%m.%Y, %H:%M")}\n'

    context.bot.send_message(chat_id=update.effective_chat.id, text=processed_data)
    logging.info(f'echo method called update: {data}')


def new_booking(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)
    logging.info(f'newbooking update: {update}')
    logging.info(f'newbooking context: {context}')
    keyboard = [[InlineKeyboardButton("Option 1", callback_data='1'),
                 InlineKeyboardButton("Option 2", callback_data='2')],
                [InlineKeyboardButton("Option 3", callback_data='3')]]

    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text('Please choose:', reply_markup=reply_markup)


def calendar_handler(update, context):
    now = datetime.datetime.now(tz=tz)
    update.message.reply_text(f'Please select a date: \nCurrent time: {now.strftime("%d.%m.%Y, %H:%M")}',
                              reply_markup=create_calendar())


def inline_handler(update, context):
    selected, date = process_calendar_selection(update, context)
    logging.info(f' update: {update.effective_chat}')
    fetch_all_bookings_for_user(update.effective_chat.id)
    if selected:
        booking_id = insert_booking(update.effective_chat.id, update.effective_chat.first_name, date)
        logging.info(f'successfully inserted booking_id: {booking_id}')
        context.bot.send_message(chat_id=update.callback_query.from_user.id,
                                 text="You selected %s" % (date.strftime("%d.%m.%Y, %H:%M")),
                                 reply_markup=ReplyKeyboardRemove())


def button(update, context):
    query = update.callback_query

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    query.answer()

    query.edit_message_text(text="Selected option: {}".format(query.data))


def create_callback_data(action, year, month, day):
    """ Create the callback data associated to each button"""
    return ";".join([action, str(year), str(month), str(day)])


def separate_callback_data(data):
    """ Separate the callback data"""
    return data.split(";")


def create_calendar(year=None, month=None):
    """
    Create an inline keyboard with the provided year and month
    :param int year: Year to use in the calendar, if None the current year is used.
    :param int month: Month to use in the calendar, if None the current month is used.
    :return: Returns the InlineKeyboardMarkup object with the calendar.
    """
    now = datetime.datetime.now(tz=tz)
    if year == None: year = now.year
    if month == None: month = now.month
    data_ignore = create_callback_data("IGNORE", year, month, 0)
    keyboard = []
    # First row - Month and Year
    row = [InlineKeyboardButton(calendar.month_name[month] + " " + str(year), callback_data=data_ignore)]
    keyboard.append(row)
    # Second row - Week Days
    row = []
    for day in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]:
        row.append(InlineKeyboardButton(day, callback_data=data_ignore))
    keyboard.append(row)

    my_calendar = calendar.monthcalendar(year, month)
    for week in my_calendar:
        row = []
        for day in week:
            if (day == 0):
                row.append(InlineKeyboardButton(" ", callback_data=data_ignore))
            else:
                row.append(InlineKeyboardButton(str(day), callback_data=create_callback_data("DAY", year, month, day)))
        keyboard.append(row)
    # Last row - Buttons
    row = [InlineKeyboardButton("<", callback_data=create_callback_data("PREV-MONTH", year, month, day)),
           InlineKeyboardButton(" ", callback_data=data_ignore),
           InlineKeyboardButton(">", callback_data=create_callback_data("NEXT-MONTH", year, month, day))]
    keyboard.append(row)

    return InlineKeyboardMarkup(keyboard)


def process_calendar_selection(update, context):
    """
    Process the callback_query. This method generates a new calendar if forward or
    backward is pressed. This method should be called inside a CallbackQueryHandler.
    :param telegram.Bot bot: The bot, as provided by the CallbackQueryHandler
    :param telegram.Update update: The update, as provided by the CallbackQueryHandler
    :return: Returns a tuple (Boolean,datetime.datetime), indicating if a date is selected
                and returning the date if so.
    """
    now = datetime.datetime.now(tz=tz)

    ret_data = (False, None)
    query = update.callback_query
    (action, year, month, day) = separate_callback_data(query.data)
    curr = datetime.datetime(int(year), int(month), 1)
    if action == "IGNORE":
        context.bot.answer_callback_query(callback_query_id=query.id)
    elif action == "DAY":
        context.bot.edit_message_text(text=query.message.text,
                                      chat_id=query.message.chat_id,
                                      message_id=query.message.message_id
                                      )
        ret_data = True, datetime.datetime(int(year), int(month), int(day), now.hour, now.minute, now.second)
    elif action == "PREV-MONTH":
        pre = curr - datetime.timedelta(days=1)
        context.bot.edit_message_text(text=query.message.text,
                                      chat_id=query.message.chat_id,
                                      message_id=query.message.message_id,
                                      reply_markup=create_calendar(int(pre.year), int(pre.month)))
    elif action == "NEXT-MONTH":
        ne = curr + datetime.timedelta(days=31)
        context.bot.edit_message_text(text=query.message.text,
                                      chat_id=query.message.chat_id,
                                      message_id=query.message.message_id,
                                      reply_markup=create_calendar(int(ne.year), int(ne.month)))
    else:
        context.bot.answer_callback_query(callback_query_id=query.id, text="Something went wrong!")
        # UNKNOWN
    return ret_data
