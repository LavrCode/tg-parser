import os
import io
import sys
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait
from telegram import Bot, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.error import TelegramError, TimedOut

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser.log", encoding='utf-8'),
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID"))
# TARGET_THREAD_ID = int(os.getenv("TARGET_THREAD_ID")) - отправка в определенный тред

TEMP_DIR = "temp_media"
os.makedirs(TEMP_DIR, exist_ok=True)

RAW_CHANNELS = [
    "https://t.me/channel_1",
    "https://t.me/channel_2"
]

def clean_channel(link: str) -> str:
    return link.replace("https://t.me/", "").replace("@", "").strip()

CHANNELS = [clean_channel(link) for link in RAW_CHANNELS]
last_post_ids = {}

user_app = Client("YOUR_SESSION_NAME", api_id=API_ID, api_hash=API_HASH)
bot = Bot(token=BOT_TOKEN)

async def retry_on_error(func, max_retries=3, retry_delay=5):
    retries = 0
    while retries < max_retries:
        try:
            return await func()
        except TimedOut:
            retries += 1
            if retries >= max_retries:
                raise
            logger.warning(f"Тайм-аут, повторная попытка {retries}/{max_retries} через {retry_delay} сек")
            await asyncio.sleep(retry_delay)
        except Exception as e:
            raise e

def truncate_text(text, source, max_length=1024):
    if len(text) + len(source) <= max_length:
        return text + source
    
    available_length = max_length - len(source) - 3
    truncated_text = text[:available_length] + "..."
    
    return truncated_text + source

async def download_media_to_memory(message):
    bytes_io = io.BytesIO()
    
    media_type = None
    if message.photo:
        media_type = "photo"
    elif message.video:
        media_type = "video"
    elif message.document:
        media_type = "document"
    else:
        return None, None, None
    
    ext = ""
    if media_type == "photo":
        ext = ".jpg"
    elif media_type == "video":
        ext = ".mp4"
    elif media_type == "document" and message.document.file_name:
        ext = os.path.splitext(message.document.file_name)[1]
    
    filename = f"temp_{message.id}{ext}"
    temp_file = os.path.join(TEMP_DIR, filename)
    
    try:
        await message.download(file_name=temp_file)
        
        if not os.path.exists(temp_file):
            logger.error(f"Файл не был создан: {temp_file}")
            return None, None, None
        
        with open(temp_file, "rb") as f:
            bytes_io.write(f.read())
        
        try:
            os.remove(temp_file)
        except Exception as e:
            logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
        
        bytes_io.seek(0)
        return bytes_io, filename, media_type
    except Exception as e:
        logger.error(f"Ошибка при загрузке медиа: {e}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass
        return None, None, None

async def forward_latest_posts():
    while True:
        for channel in CHANNELS:
            try:
                await user_app.join_chat(channel)
                await asyncio.sleep(1)

                async for msg in user_app.get_chat_history(channel, limit=5):
                    last_id = last_post_ids.get(channel, 0)
                    if msg.id <= last_id:
                        continue

                    last_post_ids[channel] = msg.id

                    caption = (msg.caption or msg.text or "").strip()
                    source = f"\n\nИсточник: https://t.me/{channel}/{msg.id}"
                    full_caption = truncate_text(caption, source, 1024)

                    media = []
                    bytes_io_objects = []

                    try:
                        if msg.media_group_id:
                            album = []
                            async for m in user_app.get_chat_history(channel):
                                if m.media_group_id == msg.media_group_id:
                                    album.append(m)
                                elif m.id < msg.id:
                                    break
                            album = list(reversed(album))

                            for i, m in enumerate(album):
                                bytes_io, filename, media_type = await download_media_to_memory(m)
                                if bytes_io:
                                    bytes_io_objects.append(bytes_io)
                                    
                                    if media_type == "photo":
                                        if i == 0:
                                            media_obj = InputMediaPhoto(media=bytes_io, caption=full_caption)
                                        else:
                                            media_obj = InputMediaPhoto(media=bytes_io)
                                    elif media_type == "video":
                                        if i == 0:
                                            media_obj = InputMediaVideo(media=bytes_io, caption=full_caption)
                                        else:
                                            media_obj = InputMediaVideo(media=bytes_io)
                                    elif media_type == "document":
                                        if i == 0:
                                            media_obj = InputMediaDocument(media=bytes_io, caption=full_caption, filename=filename)
                                        else:
                                            media_obj = InputMediaDocument(media=bytes_io, filename=filename)
                                    else:
                                        continue
                                    
                                    media.append(media_obj)

                        else:
                            bytes_io, filename, media_type = await download_media_to_memory(msg)
                            if bytes_io:
                                bytes_io_objects.append(bytes_io)
                                
                                if media_type == "photo":
                                    media.append(InputMediaPhoto(media=bytes_io, caption=full_caption))
                                elif media_type == "video":
                                    media.append(InputMediaVideo(media=bytes_io, caption=full_caption))
                                elif media_type == "document":
                                    media.append(InputMediaDocument(media=bytes_io, caption=full_caption, filename=filename))

                        if media:
                            async def send_media():
                                return await bot.send_media_group(
                                    chat_id=TARGET_CHAT_ID,
                                    media=media,
                                    # message_thread_id=TARGET_THREAD_ID
                                )
                            
                            try:
                                await retry_on_error(send_media)
                                logger.info(f"Отправлено медиа из канала {channel}")
                            except Exception as e:
                                logger.error(f"Не удалось отправить медиа из {channel}: {e}")
                                if full_caption:
                                    async def send_text():
                                        return await bot.send_message(
                                            chat_id=TARGET_CHAT_ID,
                                            text=f"Медиа не удалось загрузить\n\n{full_caption}",
                                            # message_thread_id=TARGET_THREAD_ID
                                        )
                                    await retry_on_error(send_text)
                        
                        elif full_caption:
                            async def send_text():
                                return await bot.send_message(
                                    chat_id=TARGET_CHAT_ID,
                                    text=full_caption,
                                    # message_thread_id=TARGET_THREAD_ID
                                )
                            await retry_on_error(send_text)
                            logger.info(f"Отправлено текстовое сообщение из канала {channel}")

                    except TelegramError as te:
                        logger.error(f"Ошибка Telegram Bot API: {te} {full_caption[:100]}...")
                    finally:
                        for bio in bytes_io_objects:
                            bio.close()

            except FloodWait as e:
                logger.warning(f"Flood wait: {e.value} сек для канала {channel}")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(f"Ошибка при обработке {channel}: {e}")
         
        logger.info("Завершен цикл проверки каналов, ожидание 300 секунд")
        await asyncio.sleep(300)

async def main():
    try:
        logger.info("Запуск парсера Telegram")
        async with user_app:
            await forward_latest_posts()
    except KeyboardInterrupt:
        logger.info("Парсер остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
