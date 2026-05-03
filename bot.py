from rubka import Robot, Message, filters
import time,random,asyncio,re,aiohttp,asyncio,jdatetime,aiosqlite,os




Token = os.environ.get("BOT_TOKEN", "Token")  
Data_name = "botdatabase.db"
db_lock = asyncio.Lock()
# ------- مدیریت دیتابیس سراسری -------
_db_connection = None          # اتصال واحد دیتابیس
_db_lock = asyncio.Lock()      # قفل برای دسترسی همزمان

bot = Robot(Token,max_msg_age=2000,safeSendMode=True)


# ذخیره وضعیت مسابقه در هر گروه
quiz_sessions = {}  # {chat_id: {"questions": [], "current_index": 0, "scores": {}, "active": True, "question_msg_id": None, "timeout_task": None}}
# وضعیت کاربران در حال ایجاد تیکت جدید
user_states = {}  # {user_id: {"state": "awaiting_group", "first_msg": "متن اول", "timestamp": ...}}

bot.start_save_message()

async def get_db():
    """برگرداندن اتصال سراسری دیتابیس (ایجاد می‌کند اگر وجود نداشته باشد)"""
    global _db_connection
    async with _db_lock:
        if _db_connection is None:
            _db_connection = await aiosqlite.connect(Data_name)
            await _db_connection.execute("PRAGMA journal_mode=WAL;")
            await _db_connection.execute("PRAGMA synchronous=NORMAL;")
        return _db_connection

async def connect_db():
    """استفاده از اتصال سراسری به جای اتصال جدید"""
    return await get_db()

async def create_tables():
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("PRAGMA journal_mode=WAL;")
        await cursor.execute("PRAGMA synchronous=NORMAL;")
        
        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id TEXT,
            user_id TEXT PRIMARY KEY
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS speaker_mode (
            chat_id TEXT PRIMARY KEY,
            is_enabled INTEGER DEFAULT 0
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS warning_threshold (
            chat_id TEXT PRIMARY KEY,
            threshold INTEGER DEFAULT 10
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS strict_mode (
            chat_id TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 0
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS chats (
            chat_id TEXT PRIMARY KEY,
            owner_id TEXT,
            active INTEGER DEFAULT 1
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS mutes (
            chat_id TEXT,
            user_id TEXT,
            mute_time INTEGER,
            mute_duration INTEGER,
            is_permanent INTEGER,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS warnings (
            chat_id TEXT,
            user_id TEXT,
            warning_count INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS members (
            chat_id TEXT,
            user_id TEXT,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS rules (
            chat_id TEXT,
            rule_key TEXT,
            rule_value INTEGER,
            PRIMARY KEY (chat_id, rule_key)
        )
        """)
        await db.commit()


        await cursor.execute("""
        CREATE TABLE user_stats (
            chat_id TEXT,
            user_id TEXT,
            message_count INTEGER DEFAULT 0,
            date INTEGER,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS group_lock (
            chat_id TEXT PRIMARY KEY,
            is_locked INTEGER DEFAULT 0
        )
        """)
        await db.commit()

        await cursor.execute("PRAGMA foreign_keys=off;")
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            chat_id TEXT,
            user_id TEXT,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            chat_id TEXT,
            message_id INTEGER,
            timestamp INTEGER,
            PRIMARY KEY (chat_id, message_id)
        )
        """)
        await db.commit()
        
        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT,
            status TEXT DEFAULT 'open',
            created_at INTEGER NOT NULL,
            closed_at INTEGER
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,      -- author of the message
            message_text TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            is_from_admin INTEGER DEFAULT 0,
            FOREIGN KEY (ticket_id) REFERENCES tickets (ticket_id) ON DELETE CASCADE
        )
        """)
        await db.commit()

        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS support_admins (
            chat_id TEXT,
            user_id TEXT,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()

        # جدول پیام‌های خوش‌آمد و خداحافظ
        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS custom_greetings (
            chat_id TEXT PRIMARY KEY,
            welcome_text TEXT,
            goodbye_text TEXT
        )
        """)
        await db.commit()
        
        # جدول معافیت از قانون لینک
        await cursor.execute("""
        CREATE TABLE IF NOT EXISTS link_exempt (
            chat_id TEXT,
            user_id TEXT,
            PRIMARY KEY (chat_id, user_id)
        )
        """)
        await db.commit()        

async def set_strict_mode(chat_id, value: bool):
    db = await connect_db()
    async with db.cursor() as cursor:
        
        await cursor.execute(
            "INSERT OR REPLACE INTO strict_mode (chat_id, enabled) VALUES (?, ?)",
            (chat_id, int(value))
        )
        await db.commit()

async def is_strict_mode(chat_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT enabled FROM strict_mode WHERE chat_id=?",
            (chat_id,)
        )
        row = await cursor.fetchone()
        return row and row[0] == 1

async def add_admin(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "INSERT OR IGNORE INTO admins (chat_id, user_id) VALUES (?, ?)",
            (chat_id, user_id)
        )
        await db.commit()

async def remove_admin(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "DELETE FROM admins WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        )
        await db.commit()

async def is_admin(chat_id, user_id):
    if await is_owner(chat_id, user_id):
        return True
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT 1 FROM admins WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        )
        return await cursor.fetchone() is not None

async def toggle_group_lock(chat_id, is_locked):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "INSERT OR REPLACE INTO group_lock (chat_id, is_locked) VALUES (?, ?)",
            (chat_id, is_locked)
        )
        await db.commit()

async def is_group_locked(chat_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("SELECT is_locked FROM group_lock WHERE chat_id=?", (chat_id,))
        result = await cursor.fetchone()
        return result and result[0] == 1

async def save_member(chat_id, user_id):
    attempt_count = 0
    while attempt_count < 3:
        try:
            db = await connect_db()
            async with db.cursor() as cursor:
                await cursor.execute(
                    "INSERT OR IGNORE INTO members (chat_id, user_id) VALUES (?, ?)",
                    (chat_id, user_id)
                )
                await db.commit()
            break
        except aiosqlite.OperationalError as e:
            print(f"Database is locked. Attempt {attempt_count + 1}/3...")
            attempt_count += 1
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break
    else:print("Failed to save member after 3 attempts.")

async def get_members(chat_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT user_id FROM members WHERE chat_id=?",
            (chat_id,)
        )
        members = await cursor.fetchall()
        return [i[0] for i in members]

async def increase_message_count(chat_id, user_id):
    try:
            db = await connect_db()
            async with db.cursor() as cursor:
                await db.execute('BEGIN TRANSACTION')
                await cursor.execute("""
                INSERT INTO user_stats (chat_id, user_id, message_count, date)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(chat_id, user_id)
                DO UPDATE SET message_count = message_count + 1, date = ?
                """, (chat_id, user_id, int(time.time()), int(time.time())))
                await db.commit()
    except Exception as e:
        print(f"Error in increase_message_count: {e}")

TAG_TEXTS,rules_config,RULES_FA = [
    "کجایی رفتی؟",
    "آنلاین نمیشی چرا؟",
    "یه سر بیا!",
    "چرا همیشه دیر میای؟",
    "کی برمی‌گردی؟",
    "هیچ خبری ازت نیست!",
    "منتظرت بودیم!",
    "دیر کردی بیا!",
    "یه پیامی بده دیگه!",
    "گروه رو با بی‌خبری ترک کردی!",
    "باز هم غیب شدی؟",
    "حواست کجاست؟",
    "کجا رفته‌ای که پیدات نمی‌کنیم؟",
    "چرا هیچ‌وقت آنلاین نمی‌شی؟",
    "چطور همیشه ناپدید می‌شی؟",
    "کجایید که هیچ خبری ازتون نیست؟",
    "گروه بدون شما خیلی بی‌روح شده!",
    "منتظریم بیای، خب!",
    "هیچ خبری ازت نیست!",
    "تو که همیشه می‌اومدی، چرا الان نیستی؟",
    "دلمون تنگ شده، بیا دیگه!",
    "منتظر خبری ازت هستیم!",
    "کی از ما خبر می‌گیری؟",
    "گروه بدون شما هیچ جذابیتی نداره!",
    "حواست کجاست که خبری ازت نیست؟",
    "کجا گم شدی؟",
    "بی‌خبری چه معنی می‌ده؟",
    "هرجا که هستی، بیا دیگه!",
    "گروه رو بدون تو نمی‌چرخونه!",
    "یادت رفته گروه رو؟",
    "منتظریم تو بیای تا بحث رو ادامه بدیم!",
    "پیدات نمی‌کنیم اصلاً!",
    "یادته که هنوز اینجا منتظریم؟",
    "منتظریم یه علامت ازت ببینیم!",
    "گروه بدون تو سوت و کوره!",
    "حتی یک پیام هم نمی‌فرستی؟",
    "آیا هنوز تو گروهی؟",
    "کی میای که ادامه بدیم؟",
    "یه سر بزن دیگه!",
    "کی میای تو گروه فعال بشی؟",
    "ما هنوز هم منتظریم!",
    "گروه با حضور تو تکمیل میشه!",
    "ما رو تنها گذاشتی؟",
    "چرا خبری ازت نیست؟",
    "مگه قرار نبود همیشه آنلاین باشی؟"
    "چرا غیب زدی؟",
    "بی‌خبر نرو!",
    "خبری ازت نیست!",
    "پیدات نمیشه اصلاً!",
    "کجا گم شدی؟",
    "دلمون برات تنگ شده!",
    "همیشه غایبی!",
    "چرا جواب نمیدی؟",
    "منتظریم بیای!",
    "کی برمی‌گردی؟",
    "یه پیام بده!",
    "سرت شلوغه؟",
    "حواست به ما نیست!",
    "گروه بدون تو سوت و کوره!",
    "کلاً ناپدید شدی!",
    "چرا سر نمی‌زنی؟",
    "آنلاین میشی یا نه؟",
    "یه علامت بده زنده‌ای!",
    "بازم نیستی!",
    "ما رو یادت رفته؟",
    "چرا اینقدر ساکتی؟",
    "یه سر بزن خب!",
    "کجایی که نیستی؟",
    "تو که همیشه میومدی!",
    "گروه رو ول کردی؟",
    "غیب کامل زدی!",
    "دیگه نمیای؟",
    "منتظر ظهورتیم!",
    "کجایی آخه؟",
    "دلت برای گروه تنگ نشده؟",
    "پیدات نمی‌کنیم!",
    "یه خبری از خودت بده!"
],{
    "link": True,
    "mention": True,
    "hashtag": False,
    "emoji": False,
    "only_emoji": False,
    "number": False,
    "command": False,
    "metadata": True,
    "bold": False,
    "italic": False,
    "underline": False,
    "strike": False,
    "quote": False,
    "spoiler": False,
    "code": False,
    "mono": False,
    "photo": False,
    "video": False,
    "audio": False,
    "voice": False,
    "music": False,
    "document": False,
    "archive": False,
    "executable": False,
    "font": False,
    "sticker": False,
    "forward": True,
    "contact": False,
    "location": False,
    "live_location": False,
    "poll": False,
    "anti_flood": True,
    "gif":True
},{
    "link": "لینک",
    "mention": "منشن",
    "hashtag": "هشتگ",
    "emoji": "ایموجی",
    "only_emoji": "فقط ایموجی",
    "number": "عدد",
    "command": "دستور",
    "metadata": "متادیتا",
    "bold": "بولد",
    "italic": "ایتالیک",
    "underline": "زیرخط",
    "strike": "خط خورده",
    "quote": "کوت",
    "spoiler": "اسپویلر",
    "code": "کد",
    "mono": "مونواسپیس",
    "photo": "عکس",
    "video": "ویدیو",
    "audio": "صوت",
    "voice": "ویس",
    "music": "موزیک",
    "document": "فایل",
    "archive": "فایل فشرده",
    "executable": "فایل اجرایی",
    "font": "فونت",
    "sticker": "استیکر",
    "forward": "فوروارد",
    "contact": "شماره تماس",
    "location": "لوکیشن",
    "live_location": "لوکیشن زنده",
    "poll": "نظرسنجی",
    "anti_flood": "کد هنگی",
    "gif":"گیف"
}

# لیست ۱۰۰ سوال چهارگزینه‌ای برای مسابقه
QUIZ_QUESTIONS = [
    {"question": "پایتخت ایران کدام شهر است؟", "options": ["اصفهان", "شیراز", "تهران", "مشهد"], "answer": 2},
    {"question": "کدامیک زبان برنامه‌نویسی است؟", "options": ["HTML", "CSS", "Python", "Photoshop"], "answer": 2},
    {"question": "روبیکا متعلق به کدام کشور است؟", "options": ["آمریکا", "ایران", "چین", "روسیه"], "answer": 1},
    {"question": "بزرگترین اقیانوس جهان کدام است؟", "options": ["آتلانتیک", "هند", "منجمد شمالی", "آرام"], "answer": 3},
    {"question": "کدام حیوان به \"شتر صحرا\" معروف است؟", "options": ["شتر", "بز کوهی", "مارمولک", "روباه"], "answer": 0},
    {"question": "نویسنده کتاب \"شازده کوچولو\" کیست؟", "options": ["آنتوان دو سنت‌اگزوپری", "ویکتور هوگو", "چارلز دیکنز", "جورج اورول"], "answer": 0},
    {"question": "کدام سیاره به سیاره سرخ معروف است؟", "options": ["زهره", "مریخ", "مشتری", "زحل"], "answer": 1},
    {"question": "پدر علم پزشکی جدید کیست؟", "options": ["جالینوس", "ابن سینا", "بقراط", "رازی"], "answer": 2},
    {"question": "اولین کسی که به قطب جنوب رسید؟", "options": ["آمونسن", "اسکات", "شکلاتی", "کلمب"], "answer": 0},
    {"question": "کدام کشور بیشترین جمعیت جهان را دارد؟", "options": ["هند", "چین", "آمریکا", "اندونزی"], "answer": 1},
    {"question": "گوشی آیفون توسط کدام شرکت ساخته می‌شود؟", "options": ["سامسونگ", "نوکیا", "اپل", "هواوی"], "answer": 2},
    {"question": "کوچکترین کشور جهان کدام است؟", "options": ["موناکو", "واتیکان", "سن مارینو", "مالدیو"], "answer": 1},
    {"question": "کدامیک از موارد زیر میوه است؟", "options": ["خیار", "گوجه", "سیب", "کدو"], "answer": 2},
    {"question": "سریال \"بازی تاج و تخت\" بر اساس کتاب کدام نویسنده ساخته شده؟", "options": ["جی‌کی رولینگ", "جورج آر.آر. مارتین", "استیون کینگ", "دن براون"], "answer": 1},
    {"question": "بلندترین قله جهان قبل از اورست کدام بود؟", "options": ["کی۲", "کانگچنجونگا", "ماکالو", "همان اورست"], "answer": 3},
    {"question": "کدام کشور صادرکننده بزرگ نفت است؟", "options": ["عراق", "عربستان", "ونزوئلا", "روسیه"], "answer": 1},
    {"question": "یونسکو مخفف چیست؟", "options": ["سازمان ملل", "سازمان تربیتی و علمی ملل متحد", "سازمان بهداشت جهانی", "سازمان غذا و کشاورزی"], "answer": 1},
    {"question": "اسید معده چیست؟", "options": ["اسید سیتریک", "اسید کلریدریک", "اسید سولفوریک", "اسید نیتریک"], "answer": 1},
    {"question": "کدامیک از موارد زیر ابررایانه است؟", "options": ["هوش مصنوعی", "اینترنت", "ذخیره‌ساز ابری", "ساماندهی داده"], "answer": 3},
    {"question": "غول خودروسازی آلمان کدام است؟", "options": ["بی‌ام‌و", "فولکس‌واگن", "مرسدس", "آئودی"], "answer": 1},
    {"question": "کدام حیوان می‌تواند تغییر رنگ دهد؟", "options": ["آفتاب پرست", "پلنگ", "مار", "قورباغه"], "answer": 0},
    {"question": "کدام بازی ویدئویی توسط Mojang ساخته شده؟", "options": ["فورتنایت", "ماینکرفت", "GTA", "کالاف دیوتی"], "answer": 1},
    {"question": "کدام فیلم برنده اسکار بهترین فیلم ۲۰۲۰ شد؟", "options": ["۱۹۱۷", "پارازیت", "جوکر", "روزی روزگاری در هالیوود"], "answer": 1},
    {"question": "پول رسمی کشور ژاپن چیست؟", "options": ["یوان", "وون", "ین", "دلار"], "answer": 2},
    {"question": "کدامیک از موارد زیر سریعترین حیوان است؟", "options": ["یوزپلنگ", "شیر", "پلنگ", "روباه"], "answer": 0},
    {"question": "کدامیک از پیامبران به \"خلیل الله\" معروف است؟", "options": ["ابراهیم", "موسی", "عیسی", "محمد"], "answer": 0},
    {"question": "مخترع برق چیست؟", "options": ["ادیسون", "تسلا", "نیوتن", "فرانکلین"], "answer": 0},
    {"question": "کدامیک از موارد زیر خوراکی نیست؟", "options": ["پیتزا", "ماکارونی", "سنگ", "سالاد"], "answer": 2},
    {"question": "طولانی‌ترین رود جهان کدام است؟", "options": ["نیل", "آمازون", "ینگ تسه", "میسی سی پی"], "answer": 0},
    {"question": "کدامیک از این کشورها در قاره آفریقا قرار دارد؟", "options": ["مصر", "ترکیه", "عربستان", "پاکستان"], "answer": 0},
    {"question": "گسترده‌ترین شبکه اجتماعی در ایران؟", "options": ["اینستاگرام", "تلگرام", "روبیکا", "ایتا"], "answer": 2},
    {"question": "کدامیک از اینها عنصر شیمیایی است؟", "options": ["آب", "هوا", "اکسیژن", "خاک"], "answer": 2},
    {"question": "موفق‌ترین تیم لیگ قهرمانان اروپا؟", "options": ["بارسلونا", "بایرن مونیخ", "رئال مادرید", "لیورپول"], "answer": 2},
    {"question": "کدامیک از موارد زیر ساخته دست بشر نیست؟", "options": ["آینه", "کوه", "کامپیوتر", "قلم"], "answer": 1},
    {"question": "اولین ماهواره به فضا پرتاب شد توسط؟", "options": ["آمریکا", "شوروی", "چین", "آلمان"], "answer": 1},
    {"question": "کدامیک از اینها گاز گلخانه‌ای نیست؟", "options": ["دی اکسید کربن", "متان", "اکسیژن", "نیتروس اکسید"], "answer": 2},
    {"question": "نویسنده سهگانه \"سه گانه مغز\"؟", "options": ["هاروکی موراکامی", "دانیل کانمن", "استیون پینکر", "هوشنگ مرادی کرمانی"], "answer": 1},
    {"question": "کدامیک از موارد زیر نماد رسمی المپیک است؟", "options": ["پنج حلقه", "شعله", "مدال", "پرچم سفید"], "answer": 0},
    {"question": "بزرگترین بیابان جهان کدام است؟", "options": ["صحرای آفریقا", "گبی", "ربع الخالی", "قطب جنوب"], "answer": 3},
    {"question": "کدامیک از حیوانات زیر جزو گربه‌سانان است؟", "options": ["سگ", "گرگ", "روباه", "پلنگ"], "answer": 3},
    {"question": "پایتخت کانادا کجاست؟", "options": ["ونکوور", "مونترال", "اُتاوا", "تورنتو"], "answer": 2},
    {"question": "مخترع رایانه کیست؟", "options": ["بیل گیتس", "چارلز بابیج", "استیو جابز", "تیم برنرز لی"], "answer": 1},
    {"question": "کدامیک از موارد زیر ضدویروس است؟", "options": ["XML", "SQL", "ESET", "HTML"], "answer": 2},
    {"question": "بلندترین برج جهان کدام است؟", "options": ["برج خلیفه", "برج میلاد", "برج توکیو", "برج ایفل"], "answer": 0},
    {"question": "کدامیک از اینها یک آلت موسیقی است؟", "options": ["پیانو", "ماوس", "کیبورد", "مانیتور"], "answer": 0},
    {"question": "کدامیک از موارد زیر از حبوبات است؟", "options": ["برنج", "عدس", "گندم", "جو"], "answer": 1},
    {"question": "نقاشی \"شب پرستاره\" اثر کیست؟", "options": ["پیکاسو", "مونه", "ون گوگ", "دالی"], "answer": 2},
    {"question": "اولین انسان در فضا؟", "options": ["یوری گاگارین", "نیل آرمسترانگ", "باز آلدرین", "آلن شپرد"], "answer": 0},
    {"question": "کد کشور ایران؟", "options": ["۹۸+", "۹۵+", "۹۶+", "۹۷+"], "answer": 0},
    {"question": "کدامیک از اینها برند لوازم خانگی نیست؟", "options": ["اسنوا", "بوش", "سامسونگ", "تویوتا"], "answer": 3},
    {"question": "کدام میوه بیشترین ویتامین C را دارد؟", "options": ["پرتقال", "کیوی", "گریپ فروت", "فلفل دلمه"], "answer": 1},
    {"question": "کدام کشور در جنگ جهانی دوم به متفقین نپیوست؟", "options": ["انگلیس", "فرانسه", "آلمان", "شوروی"], "answer": 2},
    {"question": "کدامیک از موارد زیر کوه نیست؟", "options": ["دماوند", "اورست", "سحابی", "البرز"], "answer": 2},
    {"question": "کدام فیلم انیمیشنی ساخته استودیو گیبلی است؟", "options": ["سفید برفی", "شهر اشباح", "توی استوری", "شرک"], "answer": 1},
    {"question": "اسید اوریک در کدام بیماری بالا می‌رود؟", "options": ["دیابت", "نقرس", "فشار خون", "میگرن"], "answer": 1},
    {"question": "گران‌ترین فلز جهانی؟", "options": ["طلا", "پلاتین", "رودیوم", "نقره"], "answer": 2},
    {"question": "شبکه اجتماعی خاص ایرانیان؟", "options": ["فیس بوک", "توییتر", "روبیکا", "لینکدین"], "answer": 2},
    {"question": "بازیگر نقش هری پاتر؟", "options": ["دنیل ردکلیف", "اما واتسون", "روپرت گرینت", "تام فلتون"], "answer": 0},
    {"question": "کدامیک از موارد زیر اتاق عمل نیست؟", "options": ["OR", "ICU", "ER", "NICU"], "answer": 2},
    {"question": "گاز اصلی هوای تنفسی؟", "options": ["اکسیژن", "نیتروژن", "دی اکسید کربن", "آرگون"], "answer": 0},
    {"question": "کدام کشور انیمه دارد؟", "options": ["چین", "کره", "ژاپن", "تایلند"], "answer": 2},
    {"question": "پایتخت برزیل؟", "options": ["ریو", "سائوپائولو", "برازیلیا", "سالوادور"], "answer": 2},
    {"question": "کدامیک از موارد زیر برای خورد و خوراک نیست؟", "options": ["کتری", "قاشق", "چنگال", "میخ"], "answer": 3},
    {"question": "اولین پیامبر اولوالعزم؟", "options": ["نوح", "ابراهیم", "موسی", "عیسی"], "answer": 0},
    {"question": "مخفف WWW چیست؟", "options": ["World Wide Web", "Web Wide World", "Wide Web World", "World Web Wide"], "answer": 0},
    {"question": "مخترع لامپ؟", "options": ["تسلا", "ادیسون", "بِل", "مورس"], "answer": 1},
    {"question": "مشهورترین بازیگر نقش جیمز باند؟", "options": ["شان کانری", "راجر مور", "دانیل کریگ", "پیرس برازنان"], "answer": 0},
    {"question": "کدامیک از موارد زیر دایناسور است؟", "options": ["T-Rex", "Shark", "Elephant", "Giraffe"], "answer": 0},
    {"question": "سلطان سلاطین لقب کدام شاه است؟", "options": ["نادرشاه", "کوروش", "داریوش", "خشایارشاه"], "answer": 0},
    {"question": "مهم‌ترین جشنواره فیلم جهان؟", "options": ["کن", "ونیز", "برلین", "اسکار"], "answer": 0},
    {"question": "کدام حیوان تخم نمی‌گذارد؟", "options": ["سگ", "مرغ", "کروکودیل", "پنگوئن"], "answer": 0},
    {"question": "بزرگترین دریاچه جهان؟", "options": ["سوپریور", "بایکال", "میشیگان", "ویکتوریا"], "answer": 0},
    {"question": "کدام خونگرم است؟", "options": ["مار", "پستاندار", "ماهی", "وزغ"], "answer": 1},
    {"question": "قله اورست در کدام کشور واقع است؟", "options": ["چین", "نپال", "هند", "پاکستان"], "answer": 1},
    {"question": "کدامیک از موارد زیر بازی نیست؟", "options": ["شطرنج", "اسنوکر", "پوکر", "کتاب"], "answer": 3},
    {"question": "جنگ‌افزار اتمی اول بار در کجا استفاده شد؟", "options": ["هیروشیما", "ناگازاکی", "هیروشیما و ناگازاکی", "اوکیناوا"], "answer": 2},
    {"question": "جنس الماس از چیست؟", "options": ["کربن", "سیلیسیم", "طلا", "آهن"], "answer": 0},
    {"question": "کشوری که دو قاره را پوشش می‌دهد؟", "options": ["مصر", "ترکیه", "روسیه", "همه موارد"], "answer": 3},
    {"question": "نود و نُه درصد باکتری‌ها بی‌ضررند؟", "options": ["True", "False", "نمی‌دانم", "فقط ۵۰٪"], "answer": 0},
    {"question": "زنبور عسل چه تولید می‌کند؟", "options": ["شیر", "عسل", "گرده", "موم"], "answer": 1},
    {"question": "مالاریا توسط چه حشره‌ای منتقل می‌شود؟", "options": ["پشه", "مگس", "ساس", "کنه"], "answer": 0},
    {"question": "کدام آهنگساز ناشنوا شد؟", "options": ["بتهوون", "موتزارت", "باخ", "شوپن"], "answer": 0},
    {"question": "کدام خوراکی از حشرات نیست؟", "options": ["عسل", "نان", "قارچ", "پنیر"], "answer": 2},
    {"question": "کوتاه‌ترین دعا در قرآن؟", "options": ["ربنا آتنا", "ربنا اغفرلی", "ربنا علیک توکلنا", "ربنا لا تزغ"], "answer": 0},
    {"question": "اولین سوره نازل شده بر پیامبر؟", "options": ["حمد", "علق", "یس", "کهف"], "answer": 1},
    {"question": "کدامیک از موارد زیر ابزار نجومی نیست؟", "options": ["تلسکوپ", "میکروسکوپ", "آینه مقعر", "عدسی"], "answer": 1},
    {"question": "کدام رنگ نور سریع‌تر است؟", "options": ["قرمز", "بنفش", "سبز", "همه یکسان"], "answer": 3},
    {"question": "بزرگترین موشک جهان؟", "options": ["استارشیپ", "ساترن V", "انرژیا", "الکترون"], "answer": 0},
    {"question": "اولین خودرو برقی انبوه ساخته شده توسط؟", "options": ["تسلا", "نیسان", "شورلت", "بی وای دی"], "answer": 1},
    {"question": "کدام اقیانوس در حال رشد است؟", "options": ["آتلانتیک", "هند", "آرام", "منجمد جنوبی"], "answer": 0},
    {"question": "بزرگترین مجسمه جهان؟", "options": ["مجسمه آزادی", "بودای لشان", "مجسمه وحدت", "مسیح نجات‌دهنده"], "answer": 2},
    {"question": "کدام سیاره حلقه دارد؟", "options": ["زحل", "مشتری", "اورانوس", "همه موارد"], "answer": 3},
    {"question": "کدامیک از موارد زیر سخت‌ترین فلز است؟", "options": ["فولاد", "تیتانیوم", "تنگستن", "کروم"], "answer": 2},
    {"question": "بزرگترین اندام بدن انسان؟", "options": ["کبد", "پوست", "مغز", "ریه"], "answer": 1},
    {"question": "اولین کسی که قهرمان شطرنج جهان شد؟", "options": ["کاسپاروف", "کارپوف", "فیشر", "شتاینیتز"], "answer": 3},
    {"question": "کشوری با بیش از ۱۷۰۰ جزیره؟", "options": ["یونان", "اندونزی", "فلیپین", "ژاپن"], "answer": 0},
    {"question": "کدام فیلم انیمیشنی برنده اسکار ۲۰۲۱ شد؟", "options": ["سول", "رایا", "لوکا", "بلو"], "answer": 0},
]


async def mute_user_db(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "INSERT OR IGNORE INTO mutes (chat_id, user_id) VALUES (?, ?)",
            (chat_id, user_id)
        )
        await db.commit()

async def clean_expired_mutes():
    while True:
        await asyncio.sleep(60)
        now = int(time.time())
        try:
            db = await connect_db()
            async with db.cursor() as cursor:
                    await cursor.execute(
                        "DELETE FROM mutes WHERE is_permanent = 0 AND mute_time + mute_duration < ?",
                        (now,)
                    )
                    await db.commit()
        except Exception as e:
            print(f"Error in clean_expired_mutes: {e}")
            
async def unmute_user_db(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "DELETE FROM mutes WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        )
        await db.commit()

async def is_muted(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT 1 FROM mutes WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        )
        return await cursor.fetchone() is not None

async def get_muted_users(chat_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT user_id FROM mutes WHERE chat_id=?",
            (chat_id,)
        )
        muted_users = await cursor.fetchall()
        return [i[0] for i in muted_users]

async def chat_exists(chat_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("SELECT 1 FROM chats WHERE chat_id=?", (chat_id,))
        return await cursor.fetchone()

async def set_owner(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "INSERT INTO chats (chat_id, owner_id) VALUES (?, ?)",
            (chat_id, user_id)
        )
        for k, v in rules_config.items():
            await cursor.execute(
                "INSERT INTO rules (chat_id, rule_key, rule_value) VALUES (?, ?, ?)",
                (chat_id, k, int(v))
            )
        await db.commit()

async def is_owner(chat_id, user_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT 1 FROM chats WHERE chat_id=? AND owner_id=?",
            (chat_id, user_id)
        )
        result = await cursor.fetchone()
        return result is not None

async def random_tag_text():
    return random.choice(TAG_TEXTS)

async def load_rules(chat_id):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("SELECT rule_key, rule_value FROM rules WHERE chat_id=?", (chat_id,))
        return {k: bool(v) for k, v in await cursor.fetchall()}

async def toggle_rule(chat_id, rule):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "UPDATE rules SET rule_value = NOT rule_value WHERE chat_id=? AND rule_key=?",
            (chat_id, rule)
        )
        await db.commit()

async def set_all_rules(chat_id, value: bool):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "UPDATE rules SET rule_value=? WHERE chat_id=?",
            (int(value), chat_id)
        )
        await db.commit()


# ---------- Helper Functions for Support System ----------
async def create_ticket(chat_id: str, user_id: str, username: str, first_msg: str):
    """Creates a new ticket and its first message, returns ticket_id."""
    now = int(time.time())
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
            INSERT INTO tickets (chat_id, user_id, username, created_at)
                VALUES (?, ?, ?, ?)
            """, (chat_id, user_id, username, now))
            ticket_id = cursor.lastrowid
            await cursor.execute("""
                INSERT INTO support_messages (ticket_id, user_id, message_text, timestamp, is_from_admin)
                VALUES (?, ?, ?, ?, 0)
            """, (ticket_id, user_id, first_msg, now))
            await db.commit()
            return ticket_id

async def add_message_to_ticket(ticket_id: int, user_id: str, message_text: str, is_admin: bool = False):
    """Adds a message to a ticket's history."""
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO support_messages (ticket_id, user_id, message_text, timestamp, is_from_admin)
                VALUES (?, ?, ?, ?, ?)
            """, (ticket_id, user_id, message_text, int(time.time()), 1 if is_admin else 0))
            await db.commit()

async def get_open_tickets(chat_id: str):
    """Returns a list of all open ticket IDs for a specific group."""
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
                SELECT ticket_id FROM tickets
                WHERE chat_id = ? AND status = 'open'
                ORDER BY created_at DESC
            """, (chat_id,))
            return [row[0] for row in await cursor.fetchall()]

async def get_ticket_info(ticket_id: int):
    """Returns full information about a specific ticket."""
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
                SELECT * FROM tickets WHERE ticket_id = ?
            """, (ticket_id,))
            return await cursor.fetchone()

async def close_ticket(ticket_id: int):
    """Closes a ticket by updating its status and setting closed_at timestamp."""
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
                UPDATE tickets SET status = 'closed', closed_at = ? WHERE ticket_id = ?
            """, (int(time.time()), ticket_id))
            await db.commit()

async def is_support_admin(chat_id: str, user_id: str):
    """Checks if a user is a support admin in a given group."""
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
                SELECT 1 FROM support_admins WHERE chat_id = ? AND user_id = ?
            """, (chat_id, user_id))
            return await cursor.fetchone() is not None

async def add_support_admin(chat_id: str, user_id: str):
    """Adds a user as a support admin for a group."""
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute("""
                INSERT OR IGNORE INTO support_admins (chat_id, user_id) VALUES (?, ?)
            """, (chat_id, user_id))
            await db.commit()
            
async def set_greeting(chat_id: str, greeting_type: str, text: str):
    """ذخیره یا به‌روزرسانی پیام خوش‌آمد یا خداحافظ"""
    db = await connect_db()
    async with db.cursor() as cursor:
        # اطمینان از وجود رکورد
        await cursor.execute(
            "INSERT OR IGNORE INTO custom_greetings (chat_id) VALUES (?)",
            (chat_id,)
        )
        if greeting_type == "welcome":
            await cursor.execute(
                "UPDATE custom_greetings SET welcome_text = ? WHERE chat_id = ?",
                (text, chat_id)
            )
        elif greeting_type == "goodbye":
            await cursor.execute(
                "UPDATE custom_greetings SET goodbye_text = ? WHERE chat_id = ?",
                (text, chat_id)
            )
        else:
            return False
        await db.commit()
    return True

async def get_greeting(chat_id: str, greeting_type: str) -> str | None:
    """دریافت پیام ذخیره‌شده (در صورت وجود)"""
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT welcome_text, goodbye_text FROM custom_greetings WHERE chat_id = ?",
            (chat_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return row[0] if greeting_type == "welcome" else row[1]

async def delete_greeting(chat_id: str, greeting_type: str):
    """حذف یک پیام سفارشی (برگشت به پیش‌فرض)"""
    db = await connect_db()
    async with db.cursor() as cursor:
        if greeting_type == "welcome":
            await cursor.execute(
                "UPDATE custom_greetings SET welcome_text = NULL WHERE chat_id = ?",
                (chat_id,)
            )
        elif greeting_type == "goodbye":
            await cursor.execute(
                "UPDATE custom_greetings SET goodbye_text = NULL WHERE chat_id = ?",
                (chat_id,)
            )
        else:
            return
        await db.commit()
        

# ---------- توابع کمکی مسابقه ----------
async def send_question(bot: Robot, chat_id: str, session: dict):
    """ارسال سوال فعلی به گروه و ذخیره message_id برای دریافت پاسخ با ریپلای"""
    q_index = session["current_index"]
    q_data = session["questions"][q_index]
    text = f"🎯 **سوال {q_index+1} از {len(session['questions'])}**\n\n"
    text += f"❓ {q_data['question']}\n\n"
    for i, opt in enumerate(q_data["options"], start=1):
        text += f"{i}️⃣ {opt}\n"
    text += "\n✅ پاسخ خود را با **ریپلای به همین پیام** و نوشتن شماره گزینه (1 تا 4) ارسال کنید."
    
    sent = await bot.send_message(chat_id, text)
    session["question_msg_id"] = sent.message_id
    return sent

async def end_quiz(bot: Robot, chat_id: str):
    """پایان مسابقه و اعلام نتایج"""
    session = quiz_sessions.pop(chat_id, None)
    if not session:
        return
    scores = session["scores"]
    if not scores:
        await bot.send_message(chat_id, "❌ هیچ کس در این مسابقه شرکت نکرد!")
        return
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    result = "🏆 **نتیجه نهایی مسابقه** 🏆\n\n"
    for i, (uid, score) in enumerate(sorted_scores[:5], 1):
        medal = ["🥇", "🥈", "🥉", "⭐", "✨"][i-1] if i <= 5 else "🔹"
        result += f"{medal} [کاربر]({uid}) — {score} امتیاز\n"
    await bot.send_message(chat_id, result)

async def cancel_quiz_timeout(bot: Robot, chat_id: str, session: dict):
    """لغو تایمر قبلی اگر وجود دارد"""
    if session.get("timeout_task"):
        session["timeout_task"].cancel()
        
# ====================== راهنمای جامع ربات (فقط ادمین/مالک) ======================
@bot.on_message(filters.text_equals("راهنما") | filters.text_equals("help"))
async def show_help(bot: Robot, message: Message):
    # فقط ادمین یا مالک گروه بتوانند ببینند
    if not (await is_admin(message.chat_id, message.sender_id) or await is_owner(message.chat_id, message.sender_id)):
        return await message.reply("❌ این دستور فقط در اختیار مدیران گروه است.")

    help_text = """
📚 **راهنمای جامع ربات مدیریت گروه** 📚

🔹 **مدیریت پایه**
• `نصب` – نصب ربات در گروه (فقط مالک)
• `افزودن ادمین` – ارتقا کاربر به ادمین کمکی (ریپلای)
• `حذف ادمین` – حذف ادمین کمکی (ریپلای)
• `لیست ادمین` – نمایش ادمین‌های کمکی

🔹 **قوانین و فیلترها** (روشن/خاموش هر قانون)
• `وضعیت` – نمایش وضعیت فعلی قوانین
• `قفل [نام قانون]` – مانند: قفل لینک ، قفل منشن ، قفل استیکر ، قفل گیف و ...
• `روشن همه` – فعال‌سازی تمام قوانین
• `خاموش همه` – غیرفعال‌سازی تمام قوانین

🔹 **حالت سختگیر**
• `حالت سختگیر روشن` – هر تخلف = اخراج فوری
• `حالت سختگیر خاموش` – فقط حذف پیام + اخطار

🔹 **سخنگوی خودکار**
• `سخنگو روشن` – پاسخ خودکار به پیام‌ها (API)
• `سخنگو خاموش` – خاموش کردن پاسخ خودکار

🔹 **سیستم اخطار و سکوت**
• `اخطار` – افزایش اخطار کاربر (ریپلای) – پس از رسیدن به حد نصاب اخراج می‌شود
• `حذف اخطار` – کاهش اخطار (ریپلای)
• `لیست اخطار` – نمایش کاربران دارای اخطار
• `تعداد اخطار [عدد]` – تعیین حد نصاب اخطار (پیش‌فرض 10)

• `سکوت [مدت ثانیه]` – سکوت موقت کاربر (ریپلای) – مثال: سکوت 60
• `سکوت دائمی` – سکوت همیشگی
• `حذف سکوت` – برداشتن سکوت (ریپلای)
• `لیست سکوت` – نمایش کاربران سکوت شده
• `پاکسازی سکوت` – حذف همه سکوت‌ها

🔹 **مدیریت پیام‌ها و گروه**
• `بن` – اخراج دائمی کاربر (ریپلای)
• `آن بن` – رفع اخراج (ریپلای)
• `حذف [تعداد]` – حذف تعداد مشخصی پیام اخیر – مثال: حذف 10
• `قفل گروه [ثانیه]` – قفل گروه برای مدت مشخص – مثال: قفل گروه 30
• `باز کردن قفل گروه` – رفع قفل دستی

🔹 **معافیت از قانون لینک**
• `معاف کردن لینک` – معاف کردن کاربر از قانون لینک (ریپلای)
• `لغو معافیت لینک` – برداشتن معافیت (ریپلای)
• `لیست معافیت لینک` – نمایش کاربران معاف

🔹 **تیکت و پشتیبانی**
• برای کاربران عادی: ارسال پیام خصوصی به ربات → متن مشکل → سپس ارسال `chat_id` گروه
• `لیست تیکت‌ها` – نمایش تیکت‌های باز گروه
• `افزودن ادمین تیکت` – اضافه کردن کاربر به عنوان ادمین پشتیبانی (ریپلای)
• در گروه پشتیبانی: ریپلای روی پیام تیکت و ارسال پاسخ → کاربر پاسخ را دریافت می‌کند
• ریپلای `بستن` روی تیکت = بستن تیکت

🔹 **تنظیمات خوش‌آمد و خداحافظ**
• `تنظیم خوش‌آمد [متن]` – متن دلخواه خوش‌آمد (از متغیرهای {name} و {chat} استفاده کنید)
• `تنظیم خداحافظ [متن]` – متن دلخواه خداحافظی
• `حذف خوش‌آمد` – بازگشت به متن پیش‌فرض
• `حذف خداحافظ` – بازگشت به متن پیش‌فرض

🔹 **سرگرمی‌ها**
• `مسابقه جدید` – شروع مسابقه سه سوال چهارگزینه‌ای
• `لغو مسابقه` – لغو مسابقه در حال اجرا
• `تاس` – انداختن تاس مجازی
• `شیر یا خط` – پرتاب سکه
• `فال` – فال حافظ با شعر و تفسیر

🔹 **آمار و اطلاعات**
• `آمار` – آمار پیام‌های یک کاربر (ریپلای)
• `آمار گروه` – گزارش کامل گروه (تعداد پیام‌ها، کاربران فعال، برترین‌ها و ...)
• `اطلاعات` یا `info` – نمایش جزئیات پیام (ریپلای)

🔹 **تگ کردن اعضا**
• `تگ` – منشن همه اعضای ذخیره شده
• `تگ [تعداد]` – منشن گروهی با حداکثر تعداد مشخص (مثال: تگ 15)

🔹 **دیگر**
• `اطلاعات` (ریپلای روی هر پیام) – دریافت مشخصات فنی پیام
• `راهنما` یا `help` – نمایش همین راهنما

━━━━━━━━━━━━━━━━━━━━━━
✅ **نکات مهم:**
- در دستوراتی که نیاز به **ریپلای** دارند، حتماً روی پیام کاربر مورد نظر ریپلای کنید.
- برای استفاده از متغیرهای {name} و {chat} در پیام خوش‌آمد، آنها را در متن خود قرار دهید.
- برای بستن تیکت توسط ادمین، روی پیام تیکت ریپلای کنید و `بستن` را بنویسید.
- در صورت نیاز به پشتیبانی بیشتر با توسعه‌دهنده تماس بگیرید.
"""
    await bot.send_message(message.chat_id, help_text, disable_web_page_preview=True)
    

@bot.on_message()
async def save_message_to_db(bot: Robot, message: Message):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        INSERT OR REPLACE INTO messages (chat_id, message_id, timestamp)
        VALUES (?, ?, ?)
        """, (message.chat_id, message.message_id, int(time.time())))
        await db.commit()

@bot.on_message()
async def speaker_reply(bot: Robot, message: Message):
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("SELECT is_enabled FROM speaker_mode WHERE chat_id=?", (message.chat_id,))
        is_enabled = await cursor.fetchone()
        if is_enabled and is_enabled[0] == 1:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://api.rubka.ir/ans/?text={message.text}") as response:
                    data = await response.json()
            if data.get("response"):
                await message.reply(data["response"])

@bot.on_message(filters.text_equals("بن"))
async def ban_user(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:
        await message.reply("⚠️ لطفاً روی پیام کاربر مورد نظر ریپلای کنید.")
        return
    data = await bot.get_message(
        chat_id=message.chat_id,
        message_id=message.reply_to_message_id
    )
    user_id = data.get("sender_id")
    if not user_id:
        return
    if await bot.ban_member_chat(chat_id=message.chat_id, user_id=user_id):
        await message.reply(
            f">🚫 **[کاربر]({user_id}) با موفقیت از گروه اخراج شد**\n"
        )

@bot.on_message(filters.text_equals("آن بن"))
async def unban_user(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:
        await message.reply("⚠️ لطفاً روی پیام کاربر مورد نظر ریپلای کنید.")
        return
    data = await bot.get_message(
        chat_id=message.chat_id,
        message_id=message.reply_to_message_id
    )
    user_id = data.get("sender_id")
    if not user_id:
        return
    if await bot.unban_chat_member(chat_id=message.chat_id, user_id=user_id):
        await message.reply(
            f">✅ **[کاربر]({user_id}) از لیست مسدودشده‌ها خارج شد**\n"
        )

@bot.on_message(filters.text_equals("حالت سختگیر روشن"))
async def strict_on(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    await set_strict_mode(message.chat_id, True)
    await message.reply(">🔥 **حالت سخت‌گیر فعال شد**\nهر تخلف = اخراج فوری")

@bot.on_message(filters.text_equals("حالت سختگیر خاموش"))
async def strict_off(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    await set_strict_mode(message.chat_id, False)
    await message.reply(">🟢 **حالت سخت‌گیر غیرفعال شد**")

@bot.on_message(filters.text_equals("سخنگو روشن"))
async def speaker_on(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        INSERT OR REPLACE INTO speaker_mode (chat_id, is_enabled) 
        VALUES (?, 1)
        """, (message.chat_id,))
        await db.commit()
    await message.reply("🔊 **سخنگو فعال شد**. از این به بعد ربات پاسخ‌ها را از سخنگو دریافت خواهد کرد.")

@bot.on_message(filters.text_equals("سخنگو خاموش"))
async def speaker_off(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        INSERT OR REPLACE INTO speaker_mode (chat_id, is_enabled) 
        VALUES (?, 0)
        """, (message.chat_id,))
        await db.commit()
    await message.reply("🔇 **سخنگو غیرفعال شد**. ربات دیگر از سخنگو استفاده نخواهد کرد.")

@bot.on_message(filters.text_contains("تعداد اخطار"))
async def set_warning_threshold(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    match = re.search(r'\d+', message.text)
    if not match:
        return await message.reply("❗ لطفاً تعداد اخطارها را به درستی وارد کنید.")
    threshold = int(match.group(0))
    if threshold <= 0:
        return await message.reply("❗ تعداد اخطار باید بزرگتر از صفر باشد.")
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        INSERT OR REPLACE INTO warning_threshold (chat_id, threshold) 
        VALUES (?, ?)
        """, (message.chat_id, threshold))
        await db.commit()
    await message.reply(f"✅ تعداد اخطارها برای این گروه به {threshold} تغییر یافت.")

@bot.on_message(filters.text_equals("اخطار"))
async def add_warning(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:
        return await message.reply("❗ لطفاً روی پیام کاربر مورد نظر ریپلای کنید.")
    data = await bot.get_message(
        chat_id=message.chat_id,
        message_id=message.reply_to_message_id
    )
    user_id = data.get("sender_id")
    if not user_id:return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        INSERT INTO warnings (chat_id, user_id, warning_count) 
        VALUES (?, ?, 1)
        ON CONFLICT(chat_id, user_id) 
        DO UPDATE SET warning_count = warning_count + 1
        """, (message.chat_id, user_id))
        await db.commit()
        await cursor.execute("SELECT warning_count FROM warnings WHERE chat_id=? AND user_id=?", (message.chat_id, user_id))
        row = await cursor.fetchone()
        warning_count = row[0] if row else 0
        await cursor.execute("SELECT threshold FROM warning_threshold WHERE chat_id=?", (message.chat_id,))
        row = await cursor.fetchone()
        threshold = row[0] if row else 10
        if warning_count >= threshold:
            await bot.ban_member_chat(chat_id=message.chat_id, user_id=user_id)
            await message.reply(f"🚫 [کاربر]({user_id}) به دلیل دریافت {threshold} اخطار از گروه اخراج شد.")
        else:
            await message.reply(f"✅ اخطار به [کاربر]({user_id}) داده شد. تعداد اخطارها: {warning_count}")

@bot.on_message(filters.text_equals("حذف اخطار"))
async def remove_warning(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:
        return await message.reply("❗ لطفاً روی پیام کاربر مورد نظر ریپلای کنید.")
    data = await bot.get_message(
        chat_id=message.chat_id,
        message_id=message.reply_to_message_id
    )
    user_id = data.get("sender_id")
    if not user_id:
        return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        UPDATE warnings SET warning_count = warning_count - 1 
        WHERE chat_id=? AND user_id=? AND warning_count > 0
        """, (message.chat_id, user_id))
        await db.commit()
        await cursor.execute("SELECT warning_count FROM warnings WHERE chat_id=? AND user_id=?", (message.chat_id, user_id))
        row = await cursor.fetchone()
        warning_count = row[0] if row else 0

    await message.reply(f"✅ اخطار از [کاربر]({user_id}) حذف شد. تعداد اخطارها: {warning_count}")

@bot.on_message(filters.text_equals("لیست اخطار"))
async def list_all_warnings(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        SELECT user_id, warning_count 
        FROM warnings 
        WHERE chat_id=? 
        ORDER BY warning_count DESC
        """, (message.chat_id,))
        warnings = await cursor.fetchall()
    if not warnings:
        return await message.reply("❗ هیچ کاربری هنوز اخطار دریافت نکرده است.")
    text = "🛑 **لیست کاربران با اخطارها**:\n\n"
    for user_id, warning_count in warnings:
        text += f"> [کاربر]({user_id}) — تعداد اخطار: {warning_count}\n"
    await message.reply(text)

@bot.on_message(filters.text_contains("حذف"))
async def delete_messages(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():return
    num_messages = int(parts[1])
    if num_messages <= 0:return await message.reply("❗ تعداد پیام‌ها باید بزرگتر از صفر باشد.")
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        SELECT message_id FROM messages WHERE chat_id=? ORDER BY timestamp DESC LIMIT ?
        """, (message.chat_id, num_messages))
        messages = await cursor.fetchall()
    if not messages:return await message.reply("❗ هیچ پیام قابل حذف در این گروه وجود ندارد.")
    for (message_id,) in messages:
        try:
            await bot.delete_message(message.chat_id, message_id)
            db = await connect_db()
            cursor = await db.cursor()
            await cursor.execute("""
                DELETE FROM messages WHERE chat_id=? AND message_id=?
                """, (message.chat_id, message_id))
            await db.commit()
        except Exception as e:
            print(f"Error deleting message {message_id}: {e}")
    await message.reply(f"✅ {num_messages} پیام اخیر حذف شد.")

@bot.on_message(filters.text_contains("قفل گروه"))
async def lock_group(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    try:
        parts = message.text.split()
        if len(parts) >= 3 and parts[2].isdigit():lock_duration = int(parts[2])
        else:return await message.reply("❗ لطفا مدت زمان قفل گروه را به درستی وارد کنید.")
        await toggle_group_lock(message.chat_id, 1)
        await message.reply(f"✅ گروه به مدت {lock_duration} ثانیه قفل شد.")
        await asyncio.sleep(lock_duration)
        await toggle_group_lock(message.chat_id, 0)
        await message.reply("✅ مدت زمان قفل گروه تمام شد. قفل گروه باز شد.")
    except ValueError:
        await message.reply("❗ لطفا مدت زمان قفل گروه را به درستی وارد کنید.")

@bot.on_message(filters.text_equals("باز کردن قفل گروه"))
async def unlock_group(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    await toggle_group_lock(message.chat_id, 0)
    await message.reply("✅ قفل گروه باز شد. پیام‌ها قابل ارسال هستند.")

@bot.on_message(filters.text_equals("افزودن ادمین"))
async def add_admin_cmd(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:return await message.reply("❗ روی پیام کاربر ریپلای کن")
    info = await bot.get_message(message.chat_id, message.reply_to_message_id)
    user_id = info["sender_id"]
    await add_admin(message.chat_id, user_id)
    await message.reply(f"✅ [کاربر]({user_id}) ادمین کمکی شد")

@bot.on_message(filters.text_equals("حذف ادمین"))
async def remove_admin_cmd(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:return await message.reply("❗ روی پیام کاربر ریپلای کن")
    info = await bot.get_message(message.chat_id, message.reply_to_message_id)
    user_id = info["sender_id"]
    await remove_admin(message.chat_id, user_id)
    await message.reply(f"❌ [کاربر]({user_id}) از ادمینی حذف شد")

@bot.on_message(filters.text_equals("لیست ادمین"))
async def list_admins(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute(
            "SELECT user_id FROM admins WHERE chat_id=?",
            (message.chat_id,)
        )
        admins = await cursor.fetchall()
    
    if not admins:
        return await message.reply("❗ ادمین کمکی وجود ندارد")
    
    text = "🛡️ **ادمین‌های کمکی :**\n\n"
    for (uid,) in admins:
        text += f">- [کاربر]({uid})\n"
    await message.reply(text)

@bot.on_message()
@bot.on_message()
async def check_group_lock(bot: Robot, message: Message):
    if not await chat_exists(message.chat_id):
        return
    if await is_group_locked(message.chat_id):
        # اگر فرستنده، ادمین یا مالک گروه است، پیام حذف نشود
        if await is_admin(message.chat_id, message.sender_id):
            return
        await message.delete()

@bot.on_message(filters.text_equals("آمار"))
async def user_stats(bot: Robot, message: Message):
    if not message.reply_to_message_id:
        return await message.reply("❗ روی پیام کاربر ریپلای کن")
    info = await bot.get_message(message.chat_id, message.reply_to_message_id)
    user_id = info["sender_id"]
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("""
        SELECT message_count FROM user_stats
        WHERE chat_id=? AND user_id=?
        """, (message.chat_id, user_id))
        row = await cursor.fetchone()
    count = row[0] if row else 0
    await message.reply(
        f"📊 **آمار کاربر**\n\n"
        f"👤 [کاربر]({user_id})\n"
        f"💬 تعداد پیام‌ها: **{count}**"
    )

@bot.on_message(filters.text_equals("آمار گروه"))
async def group_stats(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    group_name = await message.name
    now = jdatetime.datetime.now()
    time_text = now.strftime("%Y/%m/%d | %H:%M")
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("SELECT SUM(message_count) FROM user_stats WHERE chat_id=?", (message.chat_id,))
        total_messages_row = await cursor.fetchone()
        total_messages = total_messages_row[0] if total_messages_row else 0
        await cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats WHERE chat_id=?", (message.chat_id,))
        active_users_row = await cursor.fetchone()
        active_users = active_users_row[0] if active_users_row else 0
        await cursor.execute("SELECT COUNT(*) FROM admins WHERE chat_id=?", (message.chat_id,))
        admin_count_row = await cursor.fetchone()
        admin_count = admin_count_row[0] + 1 if admin_count_row else 1
        await cursor.execute("SELECT COUNT(*) FROM mutes WHERE chat_id=?", (message.chat_id,))
        muted_users_row = await cursor.fetchone()
        muted_users = muted_users_row[0] if muted_users_row else 0
        await cursor.execute("SELECT COUNT(*) FROM users WHERE chat_id=?", (message.chat_id,))
        new_members_row = await cursor.fetchone()
        new_members = new_members_row[0] if new_members_row else 0
        past_24_hours = int(time.time()) - 86400
        await cursor.execute("SELECT SUM(message_count) FROM user_stats WHERE chat_id=? AND date > ?", 
                             (message.chat_id, past_24_hours))
        daily_messages_row = await cursor.fetchone()
        daily_messages = daily_messages_row[0] if daily_messages_row else 0
        past_7_days = int(time.time()) - 604800
        await cursor.execute("SELECT SUM(message_count) FROM user_stats WHERE chat_id=? AND date > ?", 
                             (message.chat_id, past_7_days))
        weekly_messages_row = await cursor.fetchone()
        weekly_messages = weekly_messages_row[0] if weekly_messages_row else 0
        today_start = int(time.mktime(jdatetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
        await cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats WHERE chat_id=? AND date >= ?", 
                             (message.chat_id, today_start))
        new_today_count_row = await cursor.fetchone()
        new_today_count = new_today_count_row[0] if new_today_count_row else 0
        await cursor.execute("SELECT user_id, message_count FROM user_stats WHERE chat_id=? ORDER BY message_count DESC LIMIT 3", (message.chat_id,))
        top_users = await cursor.fetchall()
        medals = ["🥇", "🥈", "🥉"]
        top_text = "\n".join(
            f"> {medals[i]} [Account]({uid}) — {count} پیام"
            for i, (uid, count) in enumerate(top_users)
        )
    await message.reply(
        f"📊 **گزارش آماری — “{group_name}”**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🕒 **زمان گزارش :** {time_text}\n"
        f"👥 **اعضای کل در دیتابیس :** {new_members}\n"
        f"👤 **کاربران فعال (دارای سابقه پیام) :** {active_users}\n"
        f"🛡️ **مدیران :** {admin_count}\n"
        f"🔇 **کاربران سکوت‌شده :** {muted_users}\n"
        f"💬 **کل پیام‌ها (تاریخچه) :** {total_messages}\n"
        f"📈 **پیام‌های ۲۴ ساعت گذشته :** {daily_messages}\n"
        f"📅 **پیام‌های ۷ روز گذشته :** {weekly_messages}\n"
        f"🌟 **کاربران پیام‌دهنده امروز :** {new_today_count}\n\n"
        f"🏆 **برترین مشارکت‌کنندگان :**\n{top_text}"
    )
@bot.on_message()
async def user_messages(bot, message: Message):
    if not await chat_exists(message.chat_id):
        return
    await save_member(message.chat_id, message.sender_id)
    await increase_message_count(message.chat_id, message.sender_id)
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("SELECT mute_time, mute_duration, is_permanent FROM mutes WHERE chat_id=? AND user_id=?", 
                             (message.chat_id, message.sender_id))
        mute_info = await cursor.fetchone()
    if mute_info:
        mute_time, mute_duration, is_permanent = mute_info
        if is_permanent == 1:
            await message.delete()
            return
        remaining_time = mute_time + mute_duration - int(time.time())
        if remaining_time > 0:
            await message.delete()
        else:
            db = await connect_db()
            cursor = await db.cursor()
            await cursor.execute("DELETE FROM mutes WHERE chat_id=? AND user_id=?", (message.chat_id, message.sender_id))
            await db.commit()

@bot.on_message(filters.text_contains("تگ"))
async def tag_users(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return False

    members = await get_members(message.chat_id)
    if not members:
        return await message.reply("❗ کاربری ذخیره نشده")

    parts = message.text.split()
    chunk_size = 20
    if len(parts) == 2 and parts[1].isdigit():
        try:
            chunk_size = int(parts[1])
            if chunk_size <= 0:
                return await message.reply("❗ تعداد تگ باید بزرگتر از 0 باشد.")
        except ValueError:
            pass

    if len(members) <= chunk_size:
        chunks = [members]
    else:
        chunks = [members[i:i + chunk_size] for i in range(0, len(members), chunk_size)]

    for group in chunks:
        rand = await random_tag_text()
        text = " , ".join(f"[{rand}](tg://user?id={uid})" for uid in group)
        await bot.send_message(
            chat_id=message.chat_id,
            text=text,
            reply_to_message_id=message.message_id
        )

@bot.on_message()
async def mute_user(bot: Robot, message: Message):
    if not message.text.startswith("سکوت"):
        return
    if not await is_admin(message.chat_id, message.sender_id):return
    
    if not message.reply_to_message_id:
        return await message.reply("❗ روی پیام کاربر مورد نظر ریپلای کنید تا سکوت شود.")
    
    try:
        parts = message.text.split()
        print(parts)
        if len(parts) == 2:
            try:
                mute_duration = int(parts[1])
                is_permanent = 0
            except ValueError:
                if parts[1].lower() == "دائمی":
                    mute_duration = 0
                    is_permanent = 1
                else:
                    return await message.reply("❗ لطفا مدت زمان سکوت یا 'دائمی' را وارد کنید.")
        elif len(parts) == 3 and parts[1].lower() == "دائمی":
            mute_duration = 0
            is_permanent = 1
        else:
            return await message.reply("❗ لطفا مدت زمان سکوت یا 'دائمی' را وارد کنید.")
        info = await bot.get_message(message.chat_id, message.reply_to_message_id)
        target_id = info["sender_id"]
        print(target_id)
        db = await connect_db()
        cursor = await db.cursor()
        await cursor.execute(
            "INSERT OR REPLACE INTO mutes (chat_id, user_id, mute_time, mute_duration, is_permanent) VALUES (?, ?, ?, ?, ?)",
            (message.chat_id, target_id, int(time.time()), mute_duration, is_permanent)
        )
        await db.commit()
        if is_permanent:
            await message.reply(f"✅ [کاربر]({target_id}) برای همیشه سکوت شد.")
        else:
            await message.reply(f"✅ [کاربر]({target_id}) برای {mute_duration} ثانیه سکوت شد.")
        if mute_duration > 0:
            await asyncio.sleep(mute_duration)
            db = await connect_db()
            cursor = await db.cursor()
            await cursor.execute("DELETE FROM mutes WHERE chat_id=? AND user_id=?", (message.chat_id, target_id))
            await db.commit()
            await message.reply(f"⏳ مدت زمان سکوت برای کاربر [کاربر]({target_id}) تمام شد.")
    except ValueError as e:
        print(e)
        await message.reply("❗ لطفا مدت زمان سکوت را به درستی وارد کنید.")

@bot.on_message(filters.text_equals("پاکسازی سکوت"))
async def clear_mute_list(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    db = await connect_db()
    async with db.cursor() as cursor:
        await cursor.execute("DELETE FROM mutes WHERE chat_id=?", (message.chat_id,))
        await db.commit()
    await message.reply("✅ **لیست سکوت با موفقیت پاک شد**")

@bot.on_message(filters.text_equals("حذف سکوت"))
async def unmute_command(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    if not message.reply_to_message_id:
        return await message.reply("❗ **لطفاً روی پیام کاربر مورد نظر ریپلای کنید تا سکوت آن حذف شود**")
    info = await bot.get_message(message.chat_id, message.reply_to_message_id)
    target_id = info["sender_id"]
    await unmute_user_db(message.chat_id, target_id)  
    await message.reply("✅ **سکوت کاربر با موفقیت حذف شد**")
    await message.reply(f"🔊 سکوت [کاربر]({target_id}) برداشته شد")

@bot.on_message(filters.text_equals("لیست سکوت"))
async def mute_list(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    muted_users = await get_muted_users(message.chat_id)  
    if not muted_users:return await message.reply("✅ لیست سکوت خالی است")
    response_text = "🔇 **کاربران سکوت‌شده** :\n\n" + "\n".join(f">- [کاربر]({uid})" for uid in muted_users)
    await message.reply(response_text)

@bot.on_message(filters.text_equals("نصب") | filters.text_equals("نصب ربات"))
async def install(bot, message: Message):
    if await chat_exists(message.chat_id):  
        return False
    await set_owner(message.chat_id, message.sender_id)  
    await message.reply(f"✅ ربات با موفقیت در گروه {await message.name} نصب شد\n👑 اکنون شما مالک این چت هستید")

async def check_rules(message: Message, rules: dict):
    violations = []
    if rules.get("link") and message.has_link:violations.append("لینک")
    if rules.get("mention") and message.is_mention:violations.append("منشن")
    if rules.get("hashtag") and message.is_hashtag:violations.append("هشتگ")
    if rules.get("emoji") and message.is_emoji:violations.append("ایموجی")
    if rules.get("only_emoji") and message.is_pure_emoji:violations.append("فقط ایموجی")
    if rules.get("number") and message.is_number:violations.append("عدد")
    if rules.get("command") and message.is_command:violations.append("استفاده از دستور")
    if rules.get("metadata") and message.has_metadata:violations.append("متادیتا")
    if rules.get("bold") and message.is_bold:violations.append("متن بولد")
    if rules.get("italic") and message.is_italic:violations.append("متن ایتالیک")
    if rules.get("underline") and message.is_underline:violations.append("زیرخط")
    if rules.get("strike") and message.is_strike:violations.append("خط خورده")
    if rules.get("quote") and message.is_quote:violations.append("کوت")
    if rules.get("spoiler") and message.is_spoiler:violations.append("اسپویلر")
    if rules.get("code") and message.is_pre:violations.append("کد")
    if rules.get("mono") and message.is_mono:violations.append("مونواسپیس")
    if rules.get("photo") and message.is_photo:violations.append("عکس")
    if rules.get("video") and message.is_video:violations.append("ویدیو")
    if rules.get("audio") and message.is_audio:violations.append("صوت")
    if rules.get("voice") and message.is_voice:violations.append("ویس")
    if rules.get("music") and message.is_music:violations.append("موزیک")
    if rules.get("document") and message.is_document:violations.append("سند / فایل")
    if rules.get("archive") and message.is_archive:violations.append("فایل فشرده")
    if rules.get("executable") and message.is_executable:violations.append("فایل اجرایی")
    if rules.get("font") and message.is_font:violations.append("فونت")
    if rules.get("sticker") and message.sticker:violations.append("استیکر")
    if rules.get("forward") and message.is_forwarded:violations.append("فوروارد")
    if rules.get("contact") and message.is_contact:violations.append("شماره تماس")
    if rules.get("location") and message.is_location:violations.append("لوکیشن")
    if rules.get("live_location") and message.is_live_location:violations.append("لوکیشن زنده")
    if rules.get("poll") and message.is_poll:violations.append("نظرسنجی")
    if rules.get("gif") and message.is_gif:violations.append("گیف")
    if rules.get("anti_flood") and message.text:
        if message.text.count(".") >= 40:violations.append("کد هنگی")
    return violations

@bot.on_message()
async def strict_and_rules_handler(bot: Robot, message: Message):
    if not await chat_exists(message.chat_id):return
    if await is_admin(message.chat_id, message.sender_id):return
    rules = await load_rules(message.chat_id)  
    violations = await check_rules(message, rules)  
    if not violations:return
    if await is_strict_mode(message.chat_id):  
        await bot.ban_member_chat(
            chat_id=message.chat_id,
            user_id=message.sender_id
        )
        await message.reply(
            f"🚫 **اخراج خودکار**\n"
            f"> [کاربر]({message.sender_id}) قوانین را نقض کرد و به دلیل روشن بودن حالت سختگیر از گروه اخراج شد\n"
            f"📌 تخلف صورت گرفته : {' و '.join(violations)}"
        )
        return await message.delete()
    await message.reply(
        f"⛔ **اخطار**\n"
        f"> [کاربر]({message.sender_id}) قوانین را نقض کرد\n"
        f"📌 دلیل: {' و '.join(violations)}",
        30
    )
    await message.delete()

@bot.on_message()
async def info(bot, message: Message):
    text = message.text.strip()
    reply_id = message.reply_to_message_id
    if text in ["get", "اطلاعات", "info"] and reply_id:
        data = await bot.get_message(message.chat_id, reply_id)
        if not data:
            return await message.reply("❗ اطلاعاتی برای این پیام یافت نشد.")
        
        # استخراج فیلدها با مقدار پیش‌فرض
        sender = data.get("sender_id", "نامشخص")
        msg_text = data.get("text", "پیام بدون متن (عکس، ویس و غیره)")
        # تبدیل timestamp به تاریخ شمسی
        timestamp = data.get("date", 0)
        if timestamp:
            import jdatetime
            dt = jdatetime.datetime.fromtimestamp(timestamp)
            date_str = dt.strftime("%Y/%m/%d - %H:%M:%S")
        else:
            date_str = "نامشخص"
        msg_id = data.get("message_id", reply_id)
        reply_to = data.get("reply_to_message_id", "ندارد")
        forwarded = data.get("is_forwarded", False)
        
        # ساخت خروجی زیبا
        output = (
            f"📄 **اطلاعات پیام**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 شناسه پیام: `{msg_id}`\n"
            f"👤 فرستنده: [{sender}](tg://user?id={sender})\n"
            f"🕒 زمان: `{date_str}`\n"
            f"↩️ پاسخ به: `{reply_to if reply_to != 'ندارد' else 'هیچ'}`\n"
            f"🔄 فوروارد شده: {'✅' if forwarded else '❌'}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📝 **متن پیام:**\n"
            f"```\n{msg_text[:500]}{'...' if len(msg_text)>500 else ''}\n```"
        )
        await bot.send_message(
            chat_id=message.chat_id,
            text=output,
            reply_to_message_id=reply_id
        )

@bot.on_message()
async def admin_commands(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):return
    text = message.text.strip()
    if text == "وضعیت" or text == "قفل ها" or text == "وضعیت":
        rules = await load_rules(message.chat_id)  
        state = "\n".join(
            f"> {RULES_FA[k]}: {'✓ فعال' if v else '× غیرفعال'}"
            for k, v in rules.items()
        )
        return await message.reply(
            f"📊 **وضعیت قوانین گروه ** --{await message.name}-- :\n\n{state}\n\n"
            f"⚙️ برای تغییر وضعیت قوانین، از دستور مثال : `قفل لینک` استفاده کنید."
        )
    if text == "خاموش همه" or text == "همه خاموش":
        await set_all_rules(message.chat_id, False)  
        return await message.reply("🔕 همه قوانین خاموش شدند")
    if text == "روشن همه" or text == "همه روشن":
        await set_all_rules(message.chat_id, True)  
        return await message.reply("🔔 همه قوانین روشن شدند")
    for k, fa in RULES_FA.items():
        if text in [fa, f"قفل {fa}"]:
            await toggle_rule(message.chat_id, k)  
            return await message.reply(f"✔️ وضعیت **{fa}** تغییر کرد")

@bot.on_message(filters.text_equals("معاف کردن لینک"))
async def exempt_link_command(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    if not message.reply_to_message_id:
        return await message.reply("❗ روی پیام کاربر مورد نظر ریپلای کنید تا از قانون لینک معاف شود.")
    
    target = await bot.get_message(message.chat_id, message.reply_to_message_id)
    target_id = str(target["sender_id"])
    
    await add_link_exempt(message.chat_id, target_id)
    await message.reply(f"✅ کاربر [{target_id}](tg://user?id={target_id}) از قانون **لینک** معاف شد.")


@bot.on_message(filters.text_equals("لغو معافیت لینک"))
async def remove_exempt_link_command(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    if not message.reply_to_message_id:
        return await message.reply("❗ روی پیام کاربر مورد نظر ریپلای کنید تا معافیت او لغو شود.")
    
    target = await bot.get_message(message.chat_id, message.reply_to_message_id)
    target_id = str(target["sender_id"])
    
    await remove_link_exempt(message.chat_id, target_id)
    await message.reply(f"❌ معافیت از قانون لینک برای کاربر [{target_id}](tg://user?id={target_id}) لغو شد.")


@bot.on_message(filters.text_equals("لیست معافیت لینک"))
async def list_exempt_link_command(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    
    exempted_users = await get_link_exempted_users(message.chat_id)
    if not exempted_users:
        return await message.reply("📭 هیچ کاربری از قانون لینک معاف نیست.")
    
    text = "✅ **کاربران معاف از قانون لینک:**\n\n"
    for uid in exempted_users:
        text += f">- [کاربر](tg://user?id={uid})\n"
    await message.reply(text)
    
    
# ====================== تنظیمات پیام خوش‌آمد و خداحافظ ======================
@bot.on_message(filters.text_startswith("تنظیم خوش‌آمد"))
async def set_welcome_message_cmd(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    # حذف کلمه "تنظیم خوش‌آمد" از متن (حتی اگر با فاصله باشد)
    text = message.text.replace("تنظیم خوش‌آمد", "", 1).strip()
    if not text and message.reply_to_message_id:
        # اگر روی یک پیام ریپلای شده، متن آن پیام را بخوان
        replied = await bot.get_message(message.chat_id, message.reply_to_message_id)
        text = replied.get("text", "")
    if not text:
        return await message.reply("❗ لطفاً متن خوش‌آمد را بنویسید یا روی یک پیام ریپلای کنید.\nمثال: `تنظیم خوش‌آمد خوش آمدید دوست عزیز`")
    
    # ذخیره در دیتابیس
    chat_id = message.chat_id
    text = text.strip()
    # می‌توان از متغیرهای {name}، {chat} و ... استفاده کرد
    await set_greeting(chat_id, "welcome", text)
    await message.reply("✅ متن خوش‌آمد با موفقیت تنظیم شد.\n" + text)

@bot.on_message(filters.text_startswith("تنظیم خداحافظ"))
async def set_goodbye_message_cmd(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    text = message.text.replace("تنظیم خداحافظ", "", 1).strip()
    if not text and message.reply_to_message_id:
        replied = await bot.get_message(message.chat_id, message.reply_to_message_id)
        text = replied.get("text", "")
    if not text:
        return await message.reply("❗ لطفاً متن خداحافظ را بنویسید یا ریپلای کنید.\nمثال: `تنظیم خداحافظ خدانگهدار، امیدواریم برگردی`")
    await set_greeting(message.chat_id, "goodbye", text.strip())
    await message.reply("✅ متن خداحافظ با موفقیت تنظیم شد.\n" + text)

@bot.on_message(filters.text_startswith("حذف خوش‌آمد"))
async def remove_welcome_cmd(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    await delete_greeting(message.chat_id, "welcome")
    await message.reply("✅ متن خوش‌آمد به حالت پیش‌فرض بازگشت.")

@bot.on_message(filters.text_startswith("حذف خداحافظ"))
async def remove_goodbye_cmd(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    await delete_greeting(message.chat_id, "goodbye")
    await message.reply("✅ متن خداحافظ به حالت پیش‌فرض بازگشت.")
    
    
# ---------- User Private Message Handler for Tickets ----------
@bot.on_message(filters.private)
async def handle_private_message(bot: Robot, message: Message):
    user_id = str(message.sender_id)
    text = message.text.strip() if message.text else ""
    if not text:
        return

    # اگر کاربر در مرحلهٔ انتظار برای گروه باشد
    if user_id in user_states and user_states[user_id]["state"] == "awaiting_group":
        state = user_states.pop(user_id)
        chat_id = text.strip()
        # بررسی وجود گروه
        if not await chat_exists(chat_id):
            return await message.reply("❌ گروهی با این شناسه یافت نشد. لطفاً `chat_id` معتبر ارسال کنید.")
        # ثبت تیکت جدید با دو پیام اولیه
        ticket_id = await create_ticket(chat_id, user_id, message.username or "", state["first_msg"])
        await add_message_to_ticket(ticket_id, user_id, text, is_admin=False)
        # ارسال تیکت به گروه پشتیبانی (همان گروه)
        ticket_info = await get_ticket_info(ticket_id)
        info_text = (
            f"🎫 **تیکت جدید #{ticket_id}**\n"
            f"👤 کاربر: [{ticket_info[3] if ticket_info[3] else 'ناشناس'}](tg://user?id={user_id})\n"
            f"📅 تاریخ: {jdatetime.datetime.fromtimestamp(ticket_info[5]).strftime('%Y/%m/%d %H:%M')}\n"
            f"📝 **پیام اول:** {state['first_msg']}\n"
            f"📝 **گروه درخواستی:** {chat_id}\n\n"
            f"➖ برای پاسخ، روی این پیام ریپلای کنید.\n"
            f"➖ برای بستن تیکت، ریپلای کنید: `بستن`"
        )
        await bot.send_message(chat_id, info_text)
        await message.reply(f"✅ تیکت شما با موفقیت ایجاد شد (شماره #{ticket_id}). به زودی پاسخ داده می‌شود.")
        return

    # اگر کاربر تیکت باز داشته باشد، پیام را به تیکت اضافه می‌کنیم
    db = await connect_db()
    async with db.cursor() as cursor:
            await cursor.execute(
                "SELECT ticket_id, chat_id FROM tickets WHERE user_id = ? AND status = 'open' LIMIT 1",
                (user_id,)
            )
            ticket = await cursor.fetchone()

    if ticket:
        ticket_id, group_chat_id = ticket
        await add_message_to_ticket(ticket_id, user_id, text, is_admin=False)
        # اطلاع‌رسانی به گروه پشتیبانی
        await bot.send_message(
            group_chat_id,
            f"📩 **پیام جدید در تیکت #{ticket_id}**\n👤 کاربر: [{message.username or ''}](tg://user?id={user_id})\n📝 متن: {text}"
        )
        await message.reply("✅ پیام شما به تیکت اضافه شد.")
        return

    # هیچ تیکت بازی نیست → درخواست شناسه گروه
    user_states[user_id] = {"state": "awaiting_group", "first_msg": text, "timestamp": int(time.time())}
    await message.reply(
        "🔹 برای ایجاد تیکت، ابتدا **شناسه عددی گروه** (chat_id) مورد نظر را ارسال کنید.\n"
        "این شناسه را می‌توانید از تنظیمات گروه یا با دستور `اطلاعات` در ربات دریافت کنید."
    )
    
    
# ---------- Admin Reply Handler in Support Group ----------
@bot.on_message()
async def handle_admin_reply_in_group(bot: Robot, message: Message):
    if not message.reply_to_message_id or not await is_support_admin(message.chat_id, str(message.sender_id)):
        return

    # دریافت پیام اصلی (که تیکت را معرفی می‌کند)
    original = await bot.get_message(message.chat_id, message.reply_to_message_id)
    if not original or "🎫 **تیکت جدید #" not in original.text and "📩 **پیام جدید در تیکت #" not in original.text:
        return

    import re
    match = re.search(r"#(\d+)", original.text)
    if not match:
        return

    ticket_id = int(match.group(1))
    ticket_info = await get_ticket_info(ticket_id)
    if not ticket_info or ticket_info[4] != 'open':
        return await message.reply("❗ این تیکت دیگر باز نیست.")

    user_id = ticket_info[2]
    reply_text = message.text.strip() if message.text else ""

    # اگر ادمین دستور بستن تیکت را بدهد
    if reply_text == "بستن":
        await close_ticket(ticket_id)
        await bot.send_message(user_id, f"🔒 تیکت #{ticket_id} شما توسط پشتیبانی بسته شد.")
        await message.reply(f"✅ تیکت #{ticket_id} بسته شد.")
        # اعلام در گروه
        await bot.send_message(message.chat_id, f"🔒 تیکت #{ticket_id} توسط {message.sender_id} بسته شد.")
        return

    # ثبت پاسخ ادمین
    admin_id = str(message.sender_id)
    await add_message_to_ticket(ticket_id, admin_id, reply_text, is_admin=True)

    # ارسال پاسخ به کاربر
    await bot.send_message(
        user_id,
        f"📢 **پاسخ پشتیبانی (تیکت #{ticket_id})**\n\n{reply_text}"
    )
    await message.reply("✅ پاسخ شما برای کاربر ارسال شد.")
    
    

@bot.on_message(filters.text_contains("تنظیم گروه تیکت") | filters.text_contains("گروه تیکت"))
async def set_support_group(bot: Robot, message: Message):
    """Command for the group owner to set the support group ID."""
    if not await is_owner(message.chat_id, str(message.sender_id)):
        return
    # This command should be used in the group that will serve as the support group.
    # We can store this ID in a new table or a variable.
    # For now, let's assume we're replying in the support group itself.
    await message.reply("✅ این گروه به‌عنوان گروه پشتیبانی اصلی تنظیم شد.")

@bot.on_message(filters.text_contains("افزودن ادمین تیکت"))
async def add_ticket_admin(bot: Robot, message: Message):
    """Command to add a support admin."""
    if not await is_owner(message.chat_id, str(message.sender_id)):
        return
    if not message.reply_to_message_id:
        return await message.reply("❗ روی پیام کاربر مورد نظر ریپلای کنید تا ادمین تیکت شود.")

    target = await bot.get_message(message.chat_id, message.reply_to_message_id)
    target_id = str(target["sender_id"])

    await add_support_admin(message.chat_id, target_id)
    await message.reply(f"✅ کاربر [{target_id}](tg://user?id={target_id}) به‌عنوان ادمین پشتیبانی اضافه شد.")

@bot.on_message(filters.text_contains("لیست تیکت‌ها"))
async def list_open_tickets(bot: Robot, message: Message):
    """List all open tickets for the support group."""
    if not await is_support_admin(message.chat_id, str(message.sender_id)):
        return

    tickets = await get_open_tickets(message.chat_id)
    if not tickets:
        return await message.reply("✅ در حال حاضر هیچ تیکت باز و پاسخ‌داده‌نشده‌ای وجود ندارد.")

    text = "📋 **لیست تیکت‌های باز:**\n\n"
    for t_id in tickets:
        info = await get_ticket_info(t_id)
        if info:
            text += f"🎫 **تیکت #{t_id}** از کاربر {info[3]} (تاریخ: {time.ctime(info[5])})\n"
            text += f"➖ جهت پاسخ، روی این پیام ریپلای کنید.\n\n"

    await message.reply(text)
    
    # ---------- دستورات سرگرمی: تاس ----------
@bot.on_message(filters.text_equals("تاس") | filters.text_equals("dice") | filters.text_equals("تاس انداختن"))
async def roll_dice(bot: Robot, message: Message):
    # بررسی اینکه گروه نصب شده باشد (اختیاری)
    if not await chat_exists(message.chat_id):
        return await message.reply("❗ ابتدا ربات را با دستور `نصب` فعال کنید.")
    
    import random
    number = random.randint(1, 6)
    # شکلک‌های یونیکدی برای تاس
    dice_faces = {
        1: "⚀", 2: "⚁", 3: "⚂",
        4: "⚃", 5: "⚄", 6: "⚅"
    }
    await message.reply(f"🎲 **نتیجه تاس:** {dice_faces[number]}  |  عدد {number}")
    
    # ========== فال حافظ (سرگرمی) ==========
# لیست فال‌ها به صورت (شعر, تعبیر)
HAFEZ_FALLS = [
    ("ز کوی یار می‌آید نسیم باد نوروزی\nاز این باد ار مدد خواهی چراغ دل برافروزی", 
     "فال تو بسیار نیکوست. به زودی خبرهای خوشی به تو می‌رسد. دل را روشن نگه دار."),
    ("صوفی بیا که خرقه سالوس برکنیم\nوز شاهد قدسی صفا‌یی برافکنیم", 
     "زمان رها کردن ریا و دورویی است. صادق باش تا به مقصود برسی."),
    ("دوش دیدم که ملایک در میخانه زدند\nگل آدم بسرشتند و به پیمانه زدند", 
     "فال تو همراه با عرفان و مستی الهی است. به دنبال معنا باش."),
    ("سالها دل طلب جام جم از ما می‌کرد\nآنچه خود داشت ز بیگانه تمنا می‌کرد", 
     "آنچه می‌جویی درون خودت است. به درون سفر کن."),
    ("اگر آن ترک شیرازی به دست آرد دل ما را\nبه خال هندویش بخشم سمرقند و بخارا را", 
     "عشق و علاقه‌ای پرشور در راه است. مراقب باش دلت را به آسانی ندهی."),
    ("من اگر خارم اگر گل چمن آرایی هست\nکه از آن دست که می‌رویی تو بهائی هست", 
     "تو ارزشمندی، فارغ از آنچه دیگران می‌گویند. به خودت ایمان داشته باش."),
    ("سحرگاهان که مخمور سحر بودم\nبه تاک تاک گلم عشق می‌ورزیدم", 
     "عشق پنهانی در کمین است. صبور باش."),
    ("می‌خواهم که دستم گیرد و در کوی خراباتم نشاند\nتا در میخانه بگشایند و بر دردم درمان نهند", 
     "به دنبال راهی برای رهایی از غم‌ها هستی. فرصت خوبی پیش می‌آید."),
    ("بیا تا گل برافشانیم و می در ساغر اندازیم\nفلک را سقف بشکافیم و طرحی نو دراندازیم", 
     "فال بسیار عالی! وقت تغییر و تحول بزرگ است. اقدام کن."),
    ("درخت دوستی بنشان که کام دل به بار آرد\nنهال دشمنی برکن که رنج بیشمار آرد", 
     "مراقب روابطت باش. دوستی کن، دشمنی را رها کن."),
    ("ز عشق ناتمام ما جمال یار مستغنی است\nبه آب دیده ما لیکن احتیاج چشم تر دارد", 
     "دلدادگی‌ات بی‌نتیجه نمی‌ماند، اما صبر لازم است."),
    ("دل می‌رود ز دستم صاحبدلان خدا را\nدریابید حال من آخر شود رهایم", 
     "احساس تنهایی می‌کنی؟ به زودی آرامش به تو بازمی‌گردد."),
    ("به کوی میکده هر سالکی که ره دانست\nز تجرید ردای خود آزردن است", 
     "ریا را کنار بگذار. حقیقت را بپذیر."),
    ("صحبت حکام ظلمت شب یلداست\nنور ز خورشید جوی از مظالم مجوی", 
     "از قدرتمندان ظالم دوری کن. به نور حقیقت پناه ببر."),
    ("آنان که خاک را به نظر کیمیا کنند\nآیا بود که گوشه چشمی به ما کنند", 
     "امیدت به خدا و بزرگان، ناامیدت نمی‌کند."),
    ("این که بیتو به سر می‌شود از غصه مه‌آلود\nهمه از توست، تو خود باش که با توست همه چیز", 
     "تنهایی موقتی است. خودت را دوست داشته باش."),
    ("گفتم غم تو دارم گفتا غمت سرآید\nگفتم که ماه من شو گفتا اگر برآید", 
     "به خواسته‌ات خواهی رسید، اما نیاز به تلاش دارد."),
    ("چو بشنوی سخن اهل دل مگو که خطاست\nسخن شناس نباشد کی سخن شناس خداست", 
     "به حرف دلسوختگان گوش کن. آنان بهتر می‌دانند."),
    ("به می‌سجاده‌ام آلوده کن\nکه از رهبان ما را یادگار است", 
     "مذهب رسمی را رها کن، به عشق و صفا روی بیاور."),
    ("میازار مورکی را که دانه‌کش است\nکه جان دارد و جان شیرین خوش است", 
     "به موجودات ضعیف رحم کن. مهربانی به تو برمی‌گردد."),
]

@bot.on_message(filters.text_equals("فال") | filters.text_equals("فال حافظ"))
async def hafez_fortune(bot: Robot, message: Message):
    if not await chat_exists(message.chat_id):
        return await message.reply("❗ ابتدا ربات را با دستور `نصب` فعال کنید.")
    
    poem, interpretation = random.choice(HAFEZ_FALLS)
    text = (
        f"🍃 **فال حافظ** 🍃\n\n"
        f"📜 **شعر:**\n{poem}\n\n"
        f"📖 **تعبیر و غزل:**\n{interpretation}\n\n"
        f"✨ برای تو آرزوی خوشبختی دارم ✨"
    )
    await message.reply(text)
    
    # ---------- دستور شیر یا خط (سرگرمی) ----------
@bot.on_message(filters.text_equals("شیر یا خط") | filters.text_equals("شر یاخط") | filters.text_equals("coinflip"))
async def coin_flip(bot: Robot, message: Message):
    import random
    result = random.choice(["🦁 شیر", "📄 خط"])
    await message.reply(f"🪙 **نتیجه:** {result}")


@bot.on_message(filters.text_equals("مسابقه جدید"))
async def start_quiz(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    chat_id = message.chat_id
    if chat_id in quiz_sessions and quiz_sessions[chat_id].get("active"):
        return await message.reply("❗ در حال حاضر یک مسابقه در این گروه در جریان است. ابتدا آن را تمام کنید.")
    
    # ایجاد جلسه جدید
    import random
    questions = random.sample(QUIZ_QUESTIONS, min(3, len(QUIZ_QUESTIONS)))  # 3 سوال تصادفی
    session = {
        "questions": questions,
        "current_index": 0,
        "scores": {},
        "active": True,
        "question_msg_id": None,
        "timeout_task": None
    }
    quiz_sessions[chat_id] = session
    
    await message.reply("🎉 **مسابقه آغاز شد!**\nاولین سوال در حال ارسال است...")
    await send_question(bot, chat_id, session)
    
    # راه‌اندازی تایمر برای کل مسابقه (مثلاً 5 دقیقه کل مسابقه)
    async def global_timeout():
        await asyncio.sleep(300)  # 5 دقیقه
        if chat_id in quiz_sessions:
            await bot.send_message(chat_id, "⏰ زمان مسابقه به پایان رسید!")
            await end_quiz(bot, chat_id)
    task = asyncio.create_task(global_timeout())
    session["timeout_task"] = task
    
@bot.on_message()
async def quiz_answer_handler(bot: Robot, message: Message):
    chat_id = message.chat_id
    if chat_id not in quiz_sessions:
        return
    session = quiz_sessions[chat_id]
    if not session["active"]:
        return
    
    # بررسی اینکه آیا پیام ریپلای روی سوال فعلی است؟
    if not message.reply_to_message_id or message.reply_to_message_id != session.get("question_msg_id"):
        return
    
    # بررسی محتوای پاسخ (باید عدد 1 تا 4 باشد)
    answer_text = message.text.strip()
    if not answer_text.isdigit():
        await message.reply("❌ لطفاً فقط شماره گزینه (1، 2، 3 یا 4) را ارسال کنید.")
        return
    answer_index = int(answer_text) - 1  # تبدیل به 0-based
    if answer_index not in [0,1,2,3]:
        await message.reply("❌ شماره گزینه باید بین 1 تا 4 باشد.")
        return
    
    # بررسی صحت پاسخ
    current_q = session["questions"][session["current_index"]]
    if answer_index == current_q["answer"]:
        # پاسخ درست
        user_id = str(message.sender_id)
        old_score = session["scores"].get(user_id, 0)
        session["scores"][user_id] = old_score + 10
        await message.reply(f"✅ پاسخ صحیح! ۱۰ امتیاز گرفتید. امتیاز شما: {old_score+10}")
    else:
        await message.reply(f"❌ پاسخ نادرست! پاسخ صحیح: {current_q['options'][current_q['answer']]}")
    
    # رفتن به سوال بعدی یا پایان
    session["current_index"] += 1
    if session["current_index"] >= len(session["questions"]):
        # مسابقه تمام شد
        session["active"] = False
        if session.get("timeout_task"):
            session["timeout_task"].cancel()
        await end_quiz(bot, chat_id)
        # حذف جلسه (قبلاً در end_quiz پاپ شده)
        return
    
    # ارسال سوال بعدی
    await send_question(bot, chat_id, session)
    
@bot.on_message(filters.text_equals("لغو مسابقه"))
async def cancel_quiz(bot: Robot, message: Message):
    if not await is_admin(message.chat_id, message.sender_id):
        return
    chat_id = message.chat_id
    if chat_id not in quiz_sessions:
        return await message.reply("❗ در حال حاضر مسابقه‌ای در جریان نیست.")
    session = quiz_sessions[chat_id]
    if session.get("timeout_task"):
        session["timeout_task"].cancel()
    quiz_sessions.pop(chat_id, None)
    await message.reply("✅ مسابقه لغو شد.")
    
# ====================== رویدادهای ورود و خروج ======================
@bot.on_member_join()
async def welcome_new_member(bot: Robot, chat_id: str, user_id: str):
    """ارسال پیام خوش‌آمد به کاربر جدید"""
    # بررسی نصب بودن ربات در گروه
    if not await chat_exists(chat_id):
        return
    # دریافت پیام سفارشی، در غیر این صورت یک پیام پیش‌فرض
    custom_text = await get_greeting(chat_id, "welcome")
    if custom_text:
        # جایگزینی متغیرها
        custom_text = custom_text.replace("{name}", f"[{user_id}](tg://user?id={user_id})")
        custom_text = custom_text.replace("{chat}", f"{await bot.get_chat_name(chat_id)}")  # متد فرضی
        await bot.send_message(chat_id, custom_text)
    else:
        # پیام پیش‌فرض
        await bot.send_message(
            chat_id,
            f"🎉 خوش آمدی [کاربر](tg://user?id={user_id}) عزیز!\nامیدواریم لحظات خوبی را در گروه داشته باشی."
        )

@bot.on_member_leave()
async def goodbye_left_member(bot: Robot, chat_id: str, user_id: str):
    """ارسال پیام خداحافظ هنگام خروج کاربر"""
    if not await chat_exists(chat_id):
        return
    custom_text = await get_greeting(chat_id, "goodbye")
    if custom_text:
        custom_text = custom_text.replace("{name}", f"[{user_id}](tg://user?id={user_id})")
        await bot.send_message(chat_id, custom_text)
    else:
        await bot.send_message(
            chat_id,
            f"👋 خدانگهدار [کاربر](tg://user?id={user_id})، امیدواریم باز هم برگردی."
        )
        
        

async def main():
    await create_tables()
    # راه‌اندازی تسک پاکسازی خودکار میوت‌های منقضی شده در پس‌زمینه
    asyncio.create_task(clean_expired_mutes())
    await bot.run(sleep_time=0)
