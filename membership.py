"""
بررسی عضویت در کانال‌های اجباری — مشترک بین ربات (async) و مینی‌اپ (sync/Flask).

🐛 ریشه‌ی کندی/تایم‌اوت‌ها (قبلاً):
  • مینی‌اپ روی «هر» درخواست API یک getChatMember جداگانه برای هر کانال و پشت‌سرهم می‌زد (چند ده تماس برای باز کردن یک صفحه).
  • ربات هم کانال‌ها را پشت‌سرهم و بدون کش بررسی می‌کرد و تایم‌اوت هر تماس ۶۰ ثانیه بود؛ یک کانال کند = یک دقیقه انتظار.
  • با هر خطای شبکه، کاربرِ عضو هم «غیرعضو» حساب می‌شد و پیام اشتباه می‌گرفت.

حالا:
  ✔ نتیجه‌ی «عضو» چند دقیقه کش می‌شود و «غیرعضو» فقط چند ثانیه (تا بعد از عضو شدن سریع دوباره چک شود).
  ✔ به‌روزرسانی‌های chat_member تلگرام کش را همان لحظه درست می‌کنند (لفت/برگشت).
  ✔ کانال‌ها هم‌زمان بررسی می‌شوند، تایم‌اوت کوتاه است و فقط برای خطای شبکه یک بار تلاش مجدد می‌شود.
  ✔ اگر تلگرام جواب نداد ولی قبلاً «عضو» بودنش را می‌دانیم (کش قدیمی تا چند ساعت)، همان نتیجه استفاده می‌شود؛
    در غیر این صورت مثل قبل fail-closed (غیرعضو) می‌ماند ولی هیچ‌چیز غلط کش نمی‌شود.
  ✔ لاگ خطا محدود می‌شود تا یک کانال خراب هزاران گزارش به ادمین نفرستد.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, "") or default))
    except (TypeError, ValueError):
        return default


JOINED_TTL = _env_int("MEMBERSHIP_JOINED_TTL", 600)        # عضو بودن: ۱۰ دقیقه
NOT_JOINED_TTL = _env_int("MEMBERSHIP_NOT_JOINED_TTL", 15)  # غیرعضو بودن: ۱۵ ثانیه
STALE_GRACE = _env_int("MEMBERSHIP_STALE_GRACE", 6 * 3600)  # اگر تلگرام جواب نداد، «عضو»ِ قدیمی تا ۶ ساعت معتبر است
CALL_TIMEOUT = _env_int("MEMBERSHIP_CALL_TIMEOUT", 8)       # تایم‌اوت هر تماس (ثانیه)
RETRY_TIMEOUT = _env_int("MEMBERSHIP_RETRY_TIMEOUT", 6)     # تایم‌اوت تلاش مجدد
_MAX_ENTRIES = 50000

_lock = threading.Lock()
_cache: dict[tuple[str, int], tuple[bool, float]] = {}
_log_ts: dict[str, float] = {}


# ----------------------------------------------------------------------------
# کش
# ----------------------------------------------------------------------------
def _norm_chat(chat_id) -> str:
    s = str(chat_id).strip()
    try:
        return str(int(s))
    except (TypeError, ValueError):
        return s.lower()


def _key(chat_id, user_id) -> tuple[str, int]:
    return (_norm_chat(chat_id), int(user_id))


def _get(chat_id, user_id):
    with _lock:
        return _cache.get(_key(chat_id, user_id))


def _put(chat_id, user_id, joined: bool) -> None:
    with _lock:
        if len(_cache) >= _MAX_ENTRIES:
            cutoff = time.time() - JOINED_TTL
            for k in [k for k, (_j, ts) in _cache.items() if ts < cutoff][: _MAX_ENTRIES // 2]:
                _cache.pop(k, None)
            if len(_cache) >= _MAX_ENTRIES:
                _cache.clear()
        _cache[_key(chat_id, user_id)] = (bool(joined), time.time())


def set_status(chat_id, user_id: int, joined: bool, username: str | None = None) -> None:
    """از آپدیت chat_member تلگرام: کش را همان لحظه درست می‌کند (هم با آیدی عددی هم با @username کانال)."""
    try:
        _put(chat_id, user_id, joined)
        if username:
            _put("@" + str(username).lstrip("@"), user_id, joined)
    except Exception:
        logger.debug("membership.set_status failed", exc_info=True)


def invalidate_user(user_id: int) -> None:
    with _lock:
        for k in [k for k in _cache if k[1] == int(user_id)]:
            _cache.pop(k, None)


def clear() -> None:
    with _lock:
        _cache.clear()
        _log_ts.clear()


def _decide(chat_id, user_id, force: bool):
    """True/False اگر از کشِ تازه معلوم است، وگرنه None (باید از تلگرام پرسید)."""
    hit = _get(chat_id, user_id)
    if not hit:
        return None
    joined, ts = hit
    age = time.time() - ts
    if joined and age < JOINED_TTL:
        return True
    if (not joined) and age < NOT_JOINED_TTL and not force:
        return False
    return None


def _stale_joined(chat_id, user_id) -> bool:
    hit = _get(chat_id, user_id)
    return bool(hit and hit[0] and (time.time() - hit[1]) < STALE_GRACE)


def _log_throttled(key: str, level: int, msg: str, every: int) -> None:
    now = time.time()
    with _lock:
        if now - _log_ts.get(key, 0) < every:
            return
        _log_ts[key] = now
    logger.log(level, msg)


# ----------------------------------------------------------------------------
# نسخه‌ی async (ربات)
# ----------------------------------------------------------------------------
async def check_async(bot, channels: list, user_id: int, force: bool = False) -> list:
    """لیست کانال‌هایی را برمی‌گرداند که کاربر عضو آن‌ها نیست. force=True یعنی نتیجه‌ی «غیرعضو»ِ کش‌شده نادیده گرفته شود."""
    from aiogram.enums import ChatMemberStatus
    from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramServerError

    channels = list(channels or [])
    results: dict[int, bool] = {}
    pending: list[int] = []
    for i, ch in enumerate(channels):
        d = _decide(ch["id"], user_id, force)
        if d is None:
            pending.append(i)
        else:
            results[i] = d

    async def one(i: int) -> None:
        ch = channels[i]
        transient = (TelegramNetworkError, TelegramRetryAfter, TelegramServerError, asyncio.TimeoutError, ConnectionError)
        last: Exception | None = None
        for attempt, timeout in enumerate((CALL_TIMEOUT, RETRY_TIMEOUT)):
            try:
                member = await bot.get_chat_member(ch["id"], user_id, request_timeout=timeout)
                joined = member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED)
                _put(ch["id"], user_id, joined)
                results[i] = joined
                return
            except transient as e:
                last = e
                if isinstance(e, TelegramRetryAfter):
                    break  # محدودیت نرخ: تلاش فوری فایده ندارد
                continue
            except Exception as e:  # پاسخ قطعی تلگرام (ربات ادمین کانال نیست، کاربر پیدا نشد، ...)
                _log_throttled(f"def:{ch['id']}", logging.ERROR,
                               f"check_membership: کانال {ch['id']} قابل بررسی نیست (ربات باید ادمین کانال باشد): {e}", 600)
                results[i] = False
                return
        # خطای شبکه/تایم‌اوت
        if _stale_joined(ch["id"], user_id):
            results[i] = True
        else:
            results[i] = False
        _log_throttled(f"net:{ch['id']}", logging.WARNING,
                       f"check_membership: تلگرام برای کانال {ch['id']} جواب نداد ({type(last).__name__}: {last}); "
                       f"{'از نتیجه‌ی قبلیِ «عضو» استفاده شد' if results[i] else 'فعلاً غیرعضو فرض شد'}", 60)

    if pending:
        await asyncio.gather(*(one(i) for i in pending))
    return [ch for i, ch in enumerate(channels) if not results.get(i, False)]


# ----------------------------------------------------------------------------
# نسخه‌ی sync (Flask / مینی‌اپ)
# ----------------------------------------------------------------------------
_session = None
_pool = None
_init_lock = threading.Lock()
_stripes = [threading.Lock() for _ in range(64)]


def _get_session_and_pool():
    global _session, _pool
    if _session is None:
        with _init_lock:
            if _session is None:
                import requests
                from requests.adapters import HTTPAdapter
                from concurrent.futures import ThreadPoolExecutor
                s = requests.Session()
                adapter = HTTPAdapter(pool_connections=4, pool_maxsize=32)
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _session = s
                _pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="membership")
    return _session, _pool


def _fetch_sync(api_base: str, ch: dict, user_id: int, force: bool) -> bool:
    """یک کانال را (با singleflight و تلاش مجدد) بررسی می‌کند. True = عضو."""
    sess, _p = _get_session_and_pool()
    cid = ch["id"]
    stripe = _stripes[hash(_key(cid, user_id)) % len(_stripes)]
    with stripe:
        # درخواست هم‌زمانِ دیگری که همین را می‌پرسید ممکن است همین الان کش را پر کرده باشد
        d = _decide(cid, user_id, force)
        if d is not None:
            return d
        last = None
        for timeout in (CALL_TIMEOUT, RETRY_TIMEOUT):
            try:
                resp = sess.get(f"{api_base}/getChatMember", params={"chat_id": cid, "user_id": user_id}, timeout=timeout)
                data = resp.json()
            except Exception as e:  # شبکه/تایم‌اوت/JSON خراب
                last = e
                continue
            if data.get("ok"):
                status = (data.get("result") or {}).get("status")
                joined = status not in ("left", "kicked")
                _put(cid, user_id, joined)
                return joined
            code = int(data.get("error_code") or 0)
            if code == 429 or code >= 500:
                last = RuntimeError(f"telegram {code}")
                if code == 429:
                    break
                continue
            # پاسخ قطعی (ربات ادمین نیست، کاربر پیدا نشد، ...)
            _log_throttled(f"def:{cid}", logging.ERROR,
                           f"check_membership: کانال {cid} قابل بررسی نیست (ربات باید ادمین کانال باشد): {data.get('description')}", 600)
            return False
        joined = _stale_joined(cid, user_id)
        _log_throttled(f"net:{cid}", logging.WARNING,
                       f"check_membership(mini-app): تلگرام برای کانال {cid} جواب نداد ({last}); "
                       f"{'از نتیجه‌ی قبلیِ «عضو» استفاده شد' if joined else 'فعلاً غیرعضو فرض شد'}", 60)
        return joined


def check_sync(api_base: str, channels: list, user_id: int, force: bool = False) -> list:
    """معادل check_async برای Flask؛ api_base مثل https://api.telegram.org/bot<TOKEN>."""
    channels = list(channels or [])
    results: dict[int, bool] = {}
    pending: list[int] = []
    for i, ch in enumerate(channels):
        d = _decide(ch["id"], user_id, force)
        if d is None:
            pending.append(i)
        else:
            results[i] = d
    if len(pending) == 1:
        results[pending[0]] = _fetch_sync(api_base, channels[pending[0]], user_id, force)
    elif pending:
        _s, pool = _get_session_and_pool()
        futs = {i: pool.submit(_fetch_sync, api_base, channels[i], user_id, force) for i in pending}
        for i, f in futs.items():
            try:
                results[i] = f.result(timeout=CALL_TIMEOUT + RETRY_TIMEOUT + 4)
            except Exception:
                results[i] = _stale_joined(channels[i]["id"], user_id)
    return [ch for i, ch in enumerate(channels) if not results.get(i, False)]
