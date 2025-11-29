from telethon import TelegramClient
import os
from dotenv import load_dotenv

load_dotenv("config.env")

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

client = TelegramClient("session_scraper", API_ID, API_HASH)

async def main():
    await client.start()
    dialogs = await client.get_dialogs()

    print("\n===== REAL TELEGRAM GROUP IDs =====\n")

    for d in dialogs:
        if d.is_group or d.is_channel:
            try:
                entity = d.entity
                # REAL API usable ID
                real_id = entity.id if entity.id < 0 else -1000000000000 - entity.id
                print(f"Name: {d.name}")
                print(f"Real Usable ID: {real_id}")
                print("-" * 40)
            except:
                pass

with client:
    client.loop.run_until_complete(main())
