import os
import win32com.client
import pythoncom
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor

# اتصال به Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],
    basic_auth=("elastic", "fIZagtfzx1q6hGp9jQZm"),
)

BULK_SIZE = 1000
MAX_WORKERS = 4

# لاک‌ها
outlook_lock = threading.Lock()
counter_lock = threading.Lock()

# شمارنده‌ها
total_emails = 0
indexed_emails = 0

def clean_email_field(email_field):
    if email_field:
        return [email.strip("'") for email in email_field.split(';') if email.strip()]
    return []

def read_folder(folder, user_name):
    bulk_actions = []
    local_total = 0
    local_indexed = 0

    def index_bulk():
        nonlocal bulk_actions, local_indexed
        if bulk_actions:
            try:
                result = bulk(es, bulk_actions, stats_only=False)
                print(f"Bulk result: {result}")
                local_indexed += len(bulk_actions)
            except Exception as e:
                print(f"Bulk index failed: {e}")
            finally:
                bulk_actions = []

    try:
        messages = folder.Items
        for message in messages:
            try:
                if message.Class == 43:
                    subject = message.Subject or ""
                    sender = message.SenderName or ""
                    body = (message.Body or "").strip().replace('\n', '')
                    received = message.ReceivedTime

                    if isinstance(received, datetime):
                        received = received.strftime("%Y-%m-%dT%H:%M:%S%z")

                    email_doc = {
                        "subject": subject,
                        "sender": sender,
                        "body": body,
                        "to": clean_email_field(message.To),
                        "cc": clean_email_field(message.CC),
                        "date": received,
                        "user": user_name
                    }

                    bulk_actions.append({
                        "_index": "email_exchange",
                        "_source": email_doc
                    })

                    local_total += 1

                    if len(bulk_actions) >= BULK_SIZE:
                        index_bulk()
            except Exception as e:
                print(f"Failed to process message: {e}")

        index_bulk()

        # پردازش زیر فولدرها
        for sub_folder in folder.Folders:
            sub_total, sub_indexed = read_folder(sub_folder, user_name)
            local_total += sub_total
            local_indexed += sub_indexed

    except Exception as e:
        print(f"Error reading folder {folder.Name}: {e}")

    return local_total, local_indexed

def extract_emails_from_pst(pst_path, folder_name):
    global total_emails, indexed_emails
    pythoncom.CoInitialize()
    outlook = None
    root_folder = None

    try:
        with outlook_lock:
            outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
            outlook.AddStore(pst_path)

            for i in range(outlook.Folders.Count):
                if outlook.Folders.Item(i + 1).Name.lower() in os.path.basename(pst_path).lower():
                    root_folder = outlook.Folders.Item(i + 1)
                    break
            if not root_folder:
                root_folder = outlook.Folders.Item(outlook.Folders.Count)

        # پردازش فولدر ایمیل‌ها
        file_total, file_indexed = read_folder(root_folder, folder_name)
        print(f"[{pst_path}] Total: {file_total}, Indexed: {file_indexed}")

        with counter_lock:
            total_emails += file_total
            indexed_emails += file_indexed

    except Exception as e:
        print(f"Error processing {pst_path}: {e}")
        traceback.print_exc()

    finally:
        with outlook_lock:
            try:
                if outlook and root_folder:
                    outlook.RemoveStore(root_folder)
            except Exception as e:
                print(f"Failed to remove store for {pst_path}: {e}")

def process_pst_file(pst_path):
    folder_name = os.path.basename(os.path.dirname(pst_path))
    print(f"Processing {pst_path}")
    extract_emails_from_pst(pst_path, folder_name)

# مسیر فولدر اصلی
base_dir = r"D:\test"

# جمع‌آوری فایل‌های PST
pst_files = []
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".pst"):
            full_path = os.path.join(root, file)
            pst_files.append(full_path)

# اجرای Multi-thread
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(process_pst_file, pst_files)

# نمایش خلاصه نهایی
print("=" * 40)
print(f"Total Emails Processed: {total_emails}")
print(f"Total Emails Indexed:   {indexed_emails}")
print("=" * 40)
