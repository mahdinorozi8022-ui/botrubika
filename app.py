import os
import threading
from flask import Flask
from omega_bot import bot

app = Flask(__name__)

@app.route('/')
def index():
    return ""

@app.route('/health')
def health_check():
    return "OK"

def run_bot():
    bot.run()

if __name__ == "__main__":
    # اجرای ربات در یک نخ جداگانه
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    # نادیده گرفتن خطای رایج نهفتگی پیام
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    
    # اجرای وب‌سرور ربات برای Render
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
