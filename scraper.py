from telethon import TelegramClient
from dotenv import load_dotenv
import os
import subprocess
import sys

PY = sys.executable

load_dotenv("config.env")

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
TOTAL_GROUPS = int(os.getenv("TOTAL_GROUPS"))

client = TelegramClient("session_scraper", API_ID, API_HASH)


async def scrape_group(group_id, group_key):
    group_id = int(group_id)
    group_name = f"group{group_key}"

    # make groups folder
    os.makedirs("groups", exist_ok=True)
    os.makedirs("scraped_data", exist_ok=True)

    last_id_file = f"groups/{group_name}_last_id.txt"

    # read last ID if exists
    if os.path.exists(last_id_file):
        with open(last_id_file, "r") as f:
            last_id = int(f.read().strip())
    else:
        last_id = None

    entity = await client.get_entity(group_id)
    print(f"\n📌 Scraping {group_name} ({group_id}) | last_id = {last_id}")

    file_path = "scraped_data/all_messages.txt"
    new_last_id = last_id

    with open(file_path, "a", encoding="utf-8") as f:

        # OLDEST → NEWEST **IMPORTANT**
        async for msg in client.iter_messages(entity, reverse=True):

            if msg.id is None:
                continue

            # skip old messages
            if last_id and msg.id <= last_id:
                continue

            text = (msg.text or "").replace("\n", " ").strip()

            if text:
                f.write(text + "\n")

            new_last_id = msg.id  # update newest ID

    # save new last_id
    if new_last_id:
        with open(last_id_file, "w") as f:
            f.write(str(new_last_id))

    print(f"✔ Updated last_id for {group_name}: {new_last_id}")


async def main():
    await client.start()
    print("✔ Logged in\n")

    for i in range(1, TOTAL_GROUPS + 1):
        group_key = f"GROUP{i}"
        group_id = os.getenv(group_key)
        await scrape_group(group_id, i)

    print("\n🚀 Running email extractor...")

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

    extractor_dir = os.path.abspath(os.path.join(CURRENT_DIR, "..", "email_extractor"))
    extractor_path = os.path.join(extractor_dir, "app.py")

    print("CURRENT_DIR =", CURRENT_DIR)
    print("EXTRACTOR_DIR =", extractor_dir)
    print("EXTRACTOR_PATH =", extractor_path)

    try:
        subprocess.run([PY, extractor_path], cwd=extractor_dir, check=True)
        print("✅ Email extractor done.")
    except Exception as e:
        print("❌ Extractor error:", e)


with client:
    client.loop.run_until_complete(main())
