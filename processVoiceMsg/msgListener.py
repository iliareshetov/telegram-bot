import json
import os
import urllib
import urllib.request

import telegram
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
import logging

from booking.booking import calendar_handler, inline_handler, get_users_bookings
from processVoiceMsg.config import FOLDER_ID, IAM_TOKEN, TELEGRAM_KEY
from processVoiceMsg.dbservice import create_tables, insert_msg

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def start(update, context):
    menu = '/newbooking - забронировать время в сервисе \n' \
           '/showhistory - посмотреть историю бронирований';
    context.bot.send_message(chat_id=update.effective_chat.id, text=menu)
    logging.info(f'Start method called: {update}')
    logging.info(f'Start method called context: {context}')


def echo(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text="type: / to see the list of available commands")


def voice(update, context):
    logging.info(f'echo voice called update: {update}')
    logging.info(f'voice: {update.message.voice}')

    file = context.bot.getFile(update.message.voice.file_id)
    file.download('./voice.ogg')
    logging.info(f'type file: {type(file)}')
    with open("voice.ogg", "rb") as f:
        data = f.read()

    params = "&".join([
        "topic=general",
        "folderId=%s" % FOLDER_ID,
        "lang=ru-RU"
    ])

    url = urllib.request.Request("https://stt.api.cloud.yandex.net/speech/v1/stt:recognize?%s" % params, data=data)
    url.add_header("Authorization", "Bearer %s" % IAM_TOKEN)

    responseData = urllib.request.urlopen(url).read().decode('UTF-8')
    decodedData = json.loads(responseData)

    if decodedData.get("error_code") is None:
        msg_id = insert_msg(decodedData.get("result"))
        logging.info(f'successfully inserted msg_id: {msg_id}')
        return context.bot.send_message(chat_id=update.effective_chat.id, text=decodedData.get("result"))


def main():
    create_tables()

    updater = Updater(token=TELEGRAM_KEY, use_context=True)

    # add handlers
    start_handler = CommandHandler('start', start)
    updater.dispatcher.add_handler(start_handler)

    echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
    updater.dispatcher.add_handler(echo_handler)

    voice_handler = MessageHandler(Filters.voice, voice)
    updater.dispatcher.add_handler(voice_handler)

    new_booking_handler = CommandHandler('newbooking', calendar_handler)
    updater.dispatcher.add_handler(new_booking_handler)

    show_history_handler = CommandHandler('showhistory', get_users_bookings)
    updater.dispatcher.add_handler(show_history_handler)

    updater.dispatcher.add_handler(CallbackQueryHandler(inline_handler))

    PORT = int(os.environ.get('PORT', '8443'))

    # Start the Bot
    updater.start_webhook(listen="0.0.0.0",
                          port=PORT,
                          url_path=TELEGRAM_KEY)
    updater.bot.set_webhook("https://throw-shade-bot.herokuapp.com/" + TELEGRAM_KEY)

    # updater.start_polling()

    logging.info('Bot is running on port...')

    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()

    bot = telegram.Bot(TELEGRAM_KEY)
    # for x in range(6):
    #     print(x)
    #     bot.send_message(chat_id=1172682717, text="I'm sorry Dave I'm afraid I can't do that.")
