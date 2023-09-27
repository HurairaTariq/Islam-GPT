#from flask import Flask, request, session
from utils import *
import os, re, glob, shutil
import asyncio
from functools import partial
from collections import Counter
import zipfile
from datetime import datetime
from telegram import Update, Bot
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext
#import threading


async def handle_query(question, text):
    global trans2
    global comen2
    trans2 = 'No Verse To Show'

    if text == 'quran':
        loop = asyncio.get_running_loop()
        try:
            answer, trans3, comen3 = await asyncio.wait_for(loop.run_in_executor(None, api_fun, question), timeout=600)
            trans2 = trans3
            comen2 = comen3
            return answer
        except asyncio.TimeoutError:
            return "Sorry, the request timed out."
    elif text == 'book':
        loop = asyncio.get_running_loop()
        try:
            answer, trans3, comen3 = await asyncio.wait_for(loop.run_in_executor(None, api_funb, question), timeout=600)
            trans2 = trans3
            comen2 = comen3
            return answer
        except asyncio.TimeoutError:
            return "Sorry, the request timed out."

async def process_query(update: Update, context: CallbackContext):
    # Get user input from the Telegram update
    question = update.message.text
    ind = str(update.message.chat_id)
    print(ind, type(ind))
    liist = get_recent_data(ind)
    print(liist)
    now2 = datetime.now().isoformat()
    tame = liist[0]['time']
    if tame != '0':
        time_diff = (datetime.fromisoformat(now2) - datetime.fromisoformat(tame)).total_seconds()
        print(time_diff)
    elif tame == '0':
        time_diff = 0
    if time_diff <= 0 or time_diff > 300:
        print("Time khatam")
        reply = "I am able to answer questions and provide references Quran or Books. Type 1 for Quran and 2 for Books"
        update.message.reply_text(reply)
        context = 0
        api_fun1(ind, now2, context)
        print(reply)
    else:
        if question == '1' or question == 1:
            context = 'quran'
            api_fun1(ind, now2, context)
            reply = "Thank you for confirming, I would be providing you references from Quran now."
            update.message.reply_text(reply)
        elif question == '2' or question == 2:
            context = 'book'
            api_fun1(ind, now2, context)
            reply = "Thank you for confirming, I would be providing you references from Books now."
            update.message.reply_text(reply)
        else:
            context = 0
            api_fun1(ind, now2, context)
            last = get_recent_data(ind)
            cont = last[0]['context']
            try:
                answer = await asyncio.wait_for(handle_query(question, cont), timeout=600)
            except asyncio.TimeoutError:
                answer = "Sorry, the request timed out."
            update.message.reply_text(answer)

def telegram_bot_update(update: Update, context: CallbackContext):
    # Handle incoming Telegram bot updates
    asyncio.run(process_query(update, context))



def run_telegram_bot():
    print("Thread started")
    # Set your Telegram bot token here
    TOKEN = "6499014160:AAHywGD6UPGOaA9ANf6DBNPazL2zBcydX_c"

    # Create the Telegram Bot
    bot = Bot(token=TOKEN)

    # Create the Telegram Updater and dispatcher
    updater = Updater(TOKEN)
    dispatcher = updater.dispatcher

    # Add a message handler for the /api/qa command
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, telegram_bot_update))

    # Start the webhook with your Flask app
    updater.start_polling()
    updater.idle()





if __name__ == '__main__':
    
    # Start the Telegram bot in the main thread
    run_telegram_bot()