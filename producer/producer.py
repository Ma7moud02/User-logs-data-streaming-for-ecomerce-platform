# producer/producer.py — يبعت كل القديم بسرعة + يتابع الجديد
import time
import os
from kafka import KafkaProducer

# ====================== الإعدادات ======================
LOG_FILE = "/data/access.log"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
TOPIC = "besha-access-logs"
BATCH_SIZE = 1000        # عدد الأسطر في كل دفعة
BATCH_DELAY = 1.0        # ثانية واحدة بين كل دفعة (تقدر تخليها 0.1 لو عايز أسرع)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda x: x.encode('utf-8'),
    acks=1,
    batch_size=16384,
    linger_ms=10
)

def clean_line(line):
    line = line.rstrip('\n\r')
    while line.endswith('\\'):
        line = line[:-1].strip()
    return line.strip()

# ====================== البداية ======================
print("=" * 70)
print("besha-accesslog-producer شغال — بيبعت كل القديم دلوقتي + يتابع الجديد")
print(f"الملف   : {LOG_FILE}")
print(f"التوبيك  : {TOPIC}")
print(f"كل {BATCH_SIZE} سطر كل {BATCH_DELAY} ثانية")
print("=" * 70)

# 1. نبعت كل الأسطر القديمة من أول الملف
if os.path.exists(LOG_FILE):
    print(f"لقيت الملف! هبعت كل الأسطر القديمة دلوقتي ({LOG_FILE})")
    with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        batch = []
        line_count = 0
        for raw_line in f:
            clean_log = clean_line(raw_line)
            if clean_log:
                batch.append(clean_log)
                line_count += 1

                # لما نوصل للـ BATCH_SIZE نبعت ونستنى ثانية
                if len(batch) >= BATCH_SIZE:
                    for log in batch:
                        producer.send(TOPIC, log)
                        print(log)   # نطبع كل سطر وهو بيتبعت
                    producer.flush()
                    print(f"تم إرسال {line_count} سطر — نستنى {BATCH_DELAY} ثانية...")
                    batch = []
                    time.sleep(BATCH_DELAY)

        # نبعت الباقي لو فيه أقل من BATCH_SIZE
        if batch:
            for log in batch:
                producer.send(TOPIC, log)
                print(log)
            producer.flush()
            print(f"تم إرسال آخر {len(batch)} سطر — خلّصنا القديم!")

else:
    print(f"الملف مش موجود: {LOG_FILE} — هستنى يتخلق...")

# 2. نبدأ نتابع الجديد زي tail -f
print("دلوقتي بتابع أي سطر جديد real-time...")
with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
    f.seek(0, 2)  # نروح للآخر
    while True:
        line = f.readline()
        if not line:
            time.sleep(0.1)
            continue
        clean_log = clean_line(line)
        if clean_log:
            producer.send(TOPIC, clean_log)
            print(clean_log)