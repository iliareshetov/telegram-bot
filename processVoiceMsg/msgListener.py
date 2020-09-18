import json
import urllib
import urllib.request

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import logging


from processVoiceMsg.config import FOLDER_ID, IAM_TOKEN, TELEGRAM_KEY
from processVoiceMsg.dbservice import create_tables, insert_msg

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


def start(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text="I only understand voice messages")
    logging.info(f'Start method called: {update}')
    logging.info(f'Start method called context: {context}')


def echo(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)
    logging.info(f'echo method called update: {update}')
    logging.info(f'echo method called context: {context}')


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


def start():
    create_tables()

    updater = Updater(token=TELEGRAM_KEY, use_context=True)
    dispatcher = updater.dispatcher

    start_handler = CommandHandler('start', start)
    dispatcher.add_handler(start_handler)

    echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
    dispatcher.add_handler(echo_handler)

    voice_handler = MessageHandler(Filters.voice, voice)
    dispatcher.add_handler(voice_handler)

    updater.start_polling()
    logging.info('Bot is running ...')
