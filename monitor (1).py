"""
CGV 예매 오픈 감시 봇 v7

v6 대비 바뀐 점
- 웹훅 주소를 코드에서 제거 (반드시 Secret/환경변수로만). 알림을 2갈래로 분리:
    DISCORD_WEBHOOK_URL      : 중요 알림만 (예매 오픈 / 감시 중단 / 봇 사망)
    DISCORD_LOG_WEBHOOK_URL  : 상태·오류·하트비트 (선택. 없으면 로그 알림은 안 보냄)
- 매 확인마다 새 메시지를 보내던 것을 → 로그 채널의 메시지 1개를 수정하는 방식으로
- 디스코드 429(속도 제한) 자동 재시도
- 차단 대응: 즉시 재시작 반복(CGV를 계속 두드림) 대신, 점점 늘어나는 대기 후 재시도 → 연속 N번이면 새 서버로
- 봇 티 줄이기: webdriver 숨김, UA를 실제 브라우저 버전과 일치, 이미지/추적 차단은 기본 끔
- 알림 기록 버그 수정: 날짜가 다시 닫히면 기록을 지워서, 진짜로 열릴 때 다시 알림
- 서버 D(무료 프록시): proxy-bus 제거, 후보 늘림(socks5 포함, 전세계 목록 보조), 성공했던 프록시를 기억해서 먼저 시험
- 서버 간 간격을 서버 수에 맞춰 균등 분배 (ROSTER)
- 집/클라우드에서도 그냥 `python monitor.py` 로 실행 가능
"""
import os, re, sys, json, time, random, datetime as dt
from urllib.parse import urlparse, unquote
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

# ───────── 설정 ─────────
ALERT_HOOK = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()        # 중요 알림
LOG_HOOK = (os.getenv("DISCORD_LOG_WEBHOOK_URL") or "").strip()      # 상태/로그 (선택)
WORKER = (os.getenv("WORKER") or "").strip().lower()
TAG = f"[{WORKER.upper()}] " if WORKER else ""
ROSTER = [w for w in re.split(r"[,\s]+", (os.getenv("ROSTER") or "").lower()) if w]
BASE = os.getenv("CGV_BASE", "https://cgv.co.kr")
KEYWORD = os.getenv("KEYWORD", "요루시카")
MOVIE_TITLE = os.getenv("MOVIE_TITLE", "도작")
THEATER = os.getenv("THEATER", "홍대")
CUTOFF = dt.date.fromisoformat(os.getenv("CUTOFF_DATE", "2026-10-30"))
LOOP_MINUTES = int(os.getenv("LOOP_MINUTES") or "330")
INTERVAL = int(os.getenv("INTERVAL_SEC", "180"))
_idx = ROSTER.index(WORKER) if WORKER in ROSTER else 0
SLOT_OFFSET = INTERVAL * _idx // max(1, len(ROSTER))      # 서버끼리 균등하게 엇갈림 (a,d 두 대면 0초 / 90초)
SEND_START = os.getenv("SEND_START_MESSAGE", "false") == "true"
HEARTBEAT_MIN = int(os.getenv("HEARTBEAT_MIN", "60"))      # 정상 작동 알림 간격(분). 0이면 끔
GAP_ALERT_MIN = int(os.getenv("GAP_ALERT_MIN", "90"))
ERR_NOTICE_MIN = int(os.getenv("ERR_NOTICE_MIN", "30"))
MAX_BLOCKED = int(os.getenv("MAX_BLOCKED", "3"))
BLOCK_BACKOFF = int(os.getenv("BLOCK_BACKOFF_SEC", "60"))
SHOT_MODE = os.getenv("SHOT_MODE", "edit")                 # edit / each / off
MENTION = os.getenv("MENTION", "")
HUMAN_ENTRY = os.getenv("HUMAN_ENTRY", "true") == "true"
LIGHT_MODE = os.getenv("LIGHT_MODE", "false") == "true"    # 이미지/폰트/추적 차단. 차단 위험이 있어 기본 끔
ON_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"
PROXY_URL = (os.getenv("PROXY_URL") or "").strip()


def _proxy_conf(url):
    if not url:
        return None
    try:
        u = urlparse(url)
        conf = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
        if u.username:
            conf["username"] = unquote(u.username)
            conf["password"] = unquote(u.password or "")
        return conf
    except Exception:
        print("PROXY_URL 형식이 올바르지 않아 프록시를 쓰지 않습니다 (예: http://아이디:비번@주소:포트)")
        return None


PROXY_FREE = (os.getenv("PROXY_MODE") or "").strip().lower() == "free"
PROXY_CONF = None if PROXY_FREE else _proxy_conf(PROXY_URL)
PROXY_TRIES = int(os.getenv("PROXY_TRIES", "4"))
PROXY_SEARCH_SEC = int(os.getenv("PROXY_SEARCH_SEC", "100"))
PROXY_SEARCH_GAP_SEC = int(os.getenv("PROXY_SEARCH_GAP_SEC", "120"))
PROXY_MAX_CANDS = int(os.getenv("PROXY_MAX_CANDS", "1500"))
PROXY_ALLOW_ANY = os.getenv("PROXY_ALLOW_ANY", "true") == "true"    # 한국 목록에서 못 찾으면 전 세계 목록도 시험 (CGV 통과 여부로 거름)


def _urls(env, default):
    return [u.strip() for u in os.getenv(env, ",".join(default)).split(",") if u.strip()]


PROXY_LIST_URLS = _urls("PROXY_LIST_URLS", [
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/countries/KR/data.txt",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&country=KR&timeout=8000&ssl=all&anonymity=all",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&country=KR&timeout=8000",
])
PROXY_LIST_URLS_ANY = _urls("PROXY_LIST_URLS_ANY", [
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
])
NAV_TIMEOUT = 25000 if PROXY_FREE else 45000
PROXY_NOW = ""
BAD_PROXIES = set()

STATE_FILE = "state.json"
KST = dt.timezone(dt.timedelta(hours=9))
BLOCK_RE = re.compile(r"비정상|제한되었|접속이 제한|access denied|forbidden|Reference #", re.I)
TRACKERS = ("google-analytics", "googletagmanager", "doubleclick", "facebook.net", "criteo",
            "naver.com/wcs", "wcs.naver", "kakao.com/track", "adservice", "hotjar", "amplitude")
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']});
window.chrome = window.chrome || {runtime: {}};
"""


class Blocked(Exception):
    def __init__(self, msg="", soft=False):
        super().__init__(msg)
        self.soft = soft


class NoProxy(Exception):
    pass


class Changed(Exception):
    pass


STAGE = ""


def stage(s):
    global STAGE
    STAGE = s


# ───────── 상태 저장 ─────────
def _load():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


STATE = _load()


def set_state(**kw):
    if not any(STATE.get(k) != v for k, v in kw.items()):
        return
    STATE.update(kw)
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False)
        open("state.dirty", "w").write("1")
    except Exception as e:
        print("state save error:", e)


# ───────── 디스코드 ─────────
def stamp(msg):
    via = f"🛡️{PROXY_NOW} " if PROXY_NOW else ""
    return f"{TAG}{dt.datetime.now(KST).strftime('%H:%M:%S')} {via}{msg}"[:1900]


def _post(url, content, img=None, name="shot.jpg"):
    """디스코드 전송. 429(속도 제한)면 알려 준 시간만큼 기다렸다가 1번 재시도. 성공하면 메시지 id"""
    if not url:
        return None
    for i in (1, 2):
        try:
            if img:
                r = requests.post(url, params={"wait": "true"}, data={"content": content},
                                  files={"file": (name, img, "image/jpeg")}, timeout=30)
            else:
                r = requests.post(url, params={"wait": "true"}, json={"content": content}, timeout=15)
            if r.status_code == 429 and i == 1:
                try:
                    wait = float(r.json().get("retry_after", 2))
                except Exception:
                    wait = 2
                time.sleep(min(wait, 10) + 0.5)
                continue
            if not r.ok:
                print(f"⚠️ 디스코드 전송 실패: HTTP {r.status_code} {r.text[:120]}")
                return None
            try:
                return r.json().get("id")
            except Exception:
                return None
        except Exception as e:
            print("discord error:", str(e)[:100])
            return None
    return None


def alert(msg, img=None):
    """중요 알림 채널"""
    return _post(ALERT_HOOK, stamp(msg), img)


def log(msg, img=None):
    """로그 채널 (웹훅이 없으면 아무것도 안 함)"""
    return _post(LOG_HOOK, stamp(msg), img)


def edit_status(msg, img):
    """로그 채널의 이 서버 메시지 1개를 최신 내용으로 수정. 안 되면 새로 보냄"""
    mid, content = STATE.get("shot_msg"), stamp(msg)
    if mid:
        try:
            if img:
                r = requests.patch(f"{LOG_HOOK}/messages/{mid}",
                                   data={"payload_json": json.dumps({"content": content, "attachments": []})},
                                   files={"files[0]": ("shot.jpg", img, "image/jpeg")}, timeout=30)
            else:
                r = requests.patch(f"{LOG_HOOK}/messages/{mid}", json={"content": content}, timeout=15)
            if r.ok:
                return
            print(f"⚠️ 메시지 수정 실패: HTTP {r.status_code}")
        except Exception as e:
            print("discord edit error:", str(e)[:100])
    new = _post(LOG_HOOK, content, img)
    if new:
        set_state(shot_msg=new)


def send_status(msg, img):
    if SHOT_MODE == "off" or not LOG_HOOK:
        return
    if SHOT_MODE == "each":
        log(msg, img)
    else:
        edit_status(msg, img)


def check_webhook():
    ok = True
    for name, url, must in (("중요 알림", ALERT_HOOK, True), ("로그", LOG_HOOK, False)):
        if not url:
            print(f"{name} 웹훅 없음" + (" ← 필수! GitHub Secret DISCORD_WEBHOOK_URL 을 등록하세요" if must else " (로그 알림은 꺼짐)"))
            ok = ok and not must
            continue
        try:
            r = requests.get(url, timeout=10)
            print(f"{name} 웹훅 확인: HTTP {r.status_code}")
            if r.status_code in (401, 404):
                print(f"❌ {name} 웹훅이 없어졌거나 주소가 틀려요.")
                ok = ok and not must
        except Exception as e:
            print(f"{name} 웹훅 확인 중 오류(무시하고 계속):", str(e)[:80])
    return ok


# ───────── 날짜 줄 읽기 ─────────
DATE_PRESENT_JS = r"""
() => /(오늘|[월화수목금토일])\s*\d{1,2}/.test(document.body.innerText || '')
"""

READY_JS = r"""
(t) => { const s = document.body.innerText || '';
  return s.includes('선택 된 극장이 없습니다') || s.includes(t); }
"""

DATE_JS = r"""
() => {
  const re = /^(오늘|[월화수목금토일])\s*(\d{1,2})$/;
  const norm = s => (s || '').replace(/\s+/g, ' ').trim();
  const nums = c => (c.match(/[\d.]+/g) || []).map(Number);
  const lum = c => { const [r,g,b] = nums(c); return (0.299*r + 0.587*g + 0.114*b) / 255; };
  const isColored = c => { const n = nums(c); if (n.length < 3) return false;
    const a = n.length > 3 ? n[3] : 1; if (a < 0.05) return false;
    return (Math.max(n[0],n[1],n[2]) - Math.min(n[0],n[1],n[2])) > 60; };
  const all = Array.from(document.querySelectorAll('li, button, a, div, span'));
  const hits = all.filter(el => re.test(norm(el.innerText)));
  const leaf = hits.filter(el => !hits.some(o => o !== el && el.contains(o)));
  return leaf.map(el => {
    const day = parseInt(norm(el.innerText).match(re)[2], 10);
    const numEl = Array.from(el.querySelectorAll('*')).find(e => /^\d{1,2}$/.test(norm(e.innerText))) || el;
    let op = 1, e = numEl;
    while (e && e !== el.parentElement) { op *= parseFloat(getComputedStyle(e).opacity || '1'); e = e.parentElement; }
    const l = lum(getComputedStyle(numEl).color);
    const effLum = l * op + (1 - op);
    const group = [el, el.parentElement, ...Array.from(el.querySelectorAll('*'))].filter(Boolean);
    const selected = group.some(g => isColored(getComputedStyle(g).backgroundColor));
    const dis = !!el.closest('[disabled],[aria-disabled="true"]');
    const b = el.getBoundingClientRect();
    return { day, selected, dis, effLum: Math.round(effLum*100)/100,
             enabled: selected || (!dis && effLum < 0.6),
             rect: { x: b.x, y: b.y, w: b.width, h: b.height } };
  });
}
"""


def real_dates(day_numbers):
    today = dt.datetime.now(KST).date()
    out, cur = [], None
    for d in day_numbers:
        if cur is None:
            y, m = today.year, today.month
            if d < today.day - 1:
                m += 1
                if m == 13:
                    y, m = y + 1, 1
            cur = dt.date(y, m, d)
        else:
            nxt = cur + dt.timedelta(days=1)
            if nxt.day == d:
                cur = nxt
            else:
                y, m = (cur.year, cur.month + 1) if cur.month < 12 else (cur.year + 1, 1)
                cur = dt.date(y, m, d)
        out.append(cur)
    return out


# ───────── 브라우저 ─────────
def make_context(browser):
    # UA를 실제 브라우저 버전과 일치시키고(불일치는 봇 신호), 헤드리스 표시와 webdriver 플래그를 숨김
    ver = getattr(browser, "version", "") or "124.0.0.0"
    ctx = browser.new_context(
        locale="ko-KR", timezone_id="Asia/Seoul", viewport={"width": 1400, "height": 1000},
        user_agent=f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36",
        extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"})
    ctx.add_init_script(STEALTH_JS)
    if LIGHT_MODE:
        def handler(route):
            req = route.request
            if req.resource_type in ("image", "media", "font") or any(t in req.url for t in TRACKERS):
                return route.abort()
            return route.continue_()
        ctx.route("**/*", handler)
    return ctx


def reset_browser(browser, holder):
    for k in ("page", "ctx"):
        try:
            holder[k].close()
        except Exception:
            pass
    holder["ctx"] = make_context(browser)
    holder["page"] = holder["ctx"].new_page()
    holder["page"].set_default_timeout(20000)


def launch_browser(p, proxy=None):
    channel = os.getenv("BROWSER_CHANNEL") or None
    kw = {"proxy": proxy} if proxy else {}
    return p.chromium.launch(channel=channel, headless=True,
                             args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage",
                                   "--disable-blink-features=AutomationControlled"], **kw)


def use_proxy(holder):
    try:
        holder["browser"].close()
    except Exception:
        pass
    holder["browser"] = launch_browser(holder["pw"], PROXY_CONF)
    holder["proxy_on"] = True
    print("직접 접속이 막혀 프록시로 전환합니다")
    log("🛡️ 직접 접속이 막혀서 프록시로 전환했어요")


def body_text(page):
    try:
        return (page.inner_text("body", timeout=3000) or "")[:300].replace("\n", " ")
    except Exception:
        return ""


def detect_block(page):
    body = body_text(page)
    if BLOCK_RE.search(body):
        raise Blocked(f"접속 제한: {body[:80]}")
    if len(body.strip()) < 20:
        raise Blocked(f"빈 화면: {body[:80]}", soft=True)


def click_any(label, candidates, timeout=6000):
    last = None
    for make in candidates:
        try:
            make().click(timeout=timeout)
            return
        except Exception as e:
            last = e
    raise Changed(f"'{label}' 요소를 못 찾음: {str(last)[:120]}")


def snap(page, **kw):
    try:
        return page.screenshot(type="jpeg", quality=65, **kw)
    except Exception:
        return None


# ───────── 서버 D: 무료 프록시 찾기 ─────────
_HTML_BLOCK_RE = re.compile(r"비정상적으로|이용이 제한|access denied", re.I)


def _norm(line, default="http"):
    """한 줄에서 'scheme://ip:port' 뽑기. socks4는 제외, https는 http로 취급"""
    m = re.search(r"(?:(https?|socks4|socks5)://)?(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})", line.strip().lower())
    if not m:
        return None
    scheme = m.group(1) or default
    if scheme == "socks4":
        return None
    if scheme == "https":
        scheme = "http"
    return f"{scheme}://{m.group(2)}:{m.group(3)}"


def _fetch_list(urls):
    found = []
    for u in urls:
        try:
            r = requests.get(u, timeout=15)
            if not r.ok:
                print(f"프록시 목록 실패 HTTP {r.status_code}: {u[:70]}")
                continue
            default = "socks5" if "socks5" in u.lower() else "http"
            n0 = len(found)
            for line in r.text.splitlines():
                c = _norm(line, default)
                if c:
                    found.append(c)
            print(f"프록시 목록 {len(found) - n0}개: {u[:70]}")
        except Exception as e:
            print("프록시 목록 오류:", str(e)[:80], "|", u[:70])
    return found


def _test_proxy(addr):
    """이 프록시로 CGV 메인이 정상으로 열리는지 가볍게 시험 (socks5는 requests[socks] 필요)"""
    try:
        r = requests.get(BASE + "/", proxies={"http": addr, "https": addr}, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                                  "Accept-Language": "ko-KR,ko;q=0.9"})
        if r.status_code != 200 or len(r.text) < 2000 or _HTML_BLOCK_RE.search(r.text[:4000]):
            return None
        return addr
    except Exception:
        return None


def find_working_proxies(remembered):
    """(통과한 프록시 목록, 시험한 후보 수). 예전에 성공했던 프록시를 먼저 시험"""
    tiers = [PROXY_LIST_URLS] + ([PROXY_LIST_URLS_ANY] if PROXY_ALLOW_ANY else [])
    tested, first = 0, True
    for urls in tiers:
        fetched = [c for c in dict.fromkeys(_fetch_list(urls)) if c not in BAD_PROXIES and c not in remembered]
        random.shuffle(fetched)
        cands = ([c for c in remembered if c not in BAD_PROXIES] if first else []) + fetched
        first = False
        cands = cands[:PROXY_MAX_CANDS]
        tested += len(cands)
        print(f"프록시 후보 {len(cands)}개를 시험합니다 (최대 {PROXY_SEARCH_SEC}초)")
        good = []
        ex = ThreadPoolExecutor(max_workers=150)
        futs = [ex.submit(_test_proxy, c) for c in cands]
        try:
            for f in as_completed(futs, timeout=PROXY_SEARCH_SEC):
                r = f.result()
                if r:
                    good.append(r)
                    if len(good) >= 6:
                        break
        except Exception:
            pass
        ex.shutdown(wait=False, cancel_futures=True)
        print(f"통과한 프록시 {len(good)}개: {good}")
        if good:
            return good, tested
    return [], tested


def remember_good(proxy):
    lst = [proxy] + [x for x in STATE.get("good_proxies", []) if x != proxy]
    set_state(good_proxies=lst[:10])


def forget_proxy(proxy):
    BAD_PROXIES.add(proxy)
    set_state(good_proxies=[x for x in STATE.get("good_proxies", []) if x != proxy])


def refill_pool(holder, force=False):
    now = time.time()
    if not force and now - holder["last_search"] < PROXY_SEARCH_GAP_SEC:
        return
    holder["last_search"] = now
    stage("무료 프록시 찾기")
    good, holder["tested"] = find_working_proxies(STATE.get("good_proxies", []))
    for g in good:
        if g not in holder["pool"] and g not in BAD_PROXIES:
            holder["pool"].append(g)


def ensure_free_proxy(holder):
    global PROXY_NOW
    if holder.get("proxy"):
        return holder["proxy"]
    pool = [p for p in holder["pool"] if p not in BAD_PROXIES]
    if not pool:
        refill_pool(holder)
        pool = [p for p in holder["pool"] if p not in BAD_PROXIES]
    if not pool:
        PROXY_NOW = ""
        return None
    proxy = pool[0]
    try:
        holder["browser"].close()
    except Exception:
        pass
    holder["browser"] = launch_browser(holder["pw"], {"server": proxy})
    reset_browser(holder["browser"], holder)
    holder["proxy"] = proxy
    PROXY_NOW = proxy.split("://", 1)[-1]
    print("프록시 사용:", proxy)
    return proxy


def one_check_free(holder):
    """서버 D의 확인 1회: 프록시가 안 되면 다음 프록시로 바꿔 가며 최대 PROXY_TRIES번"""
    last = None
    for _ in range(PROXY_TRIES):
        proxy = ensure_free_proxy(holder)
        if not proxy:
            raise NoProxy(f"통과하는 무료 프록시를 못 찾았어요 (후보 {holder.get('tested', 0)}개 시험)")
        try:
            res = one_check(holder["browser"], holder, attempts=(1,))
            remember_good(proxy)
            return res
        except Exception as e:
            last = e
            print(f"프록시 {proxy} 실패 → 다음 후보로: {str(e)[:100]}")
            forget_proxy(proxy)
            holder["proxy"] = None
    raise last


# ───────── 예매 화면 열기 ─────────
def enter_search_like_human(page):
    stage("메인 페이지 접속")
    resp = page.goto(BASE, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    if resp is not None and resp.status in (403, 429):
        raise Blocked(f"메인 페이지부터 HTTP {resp.status}")
    page.wait_for_timeout(1000)
    if BLOCK_RE.search(body_text(page)):
        raise Blocked(f"메인 페이지 접속 제한: {body_text(page)[:80]}")

    stage("메인 페이지 둘러보기")
    try:
        page.mouse.move(400 + random.randint(0, 300), 250 + random.randint(0, 200), steps=8)
        page.wait_for_timeout(random.randint(1500, 3000))
        page.mouse.wheel(0, random.randint(150, 400))
        page.wait_for_timeout(random.randint(500, 1200))
    except Exception:
        pass

    stage("검색 화면으로 이동")
    for make in (lambda: page.locator("a[href*='itgrSrch']").first,
                 lambda: page.get_by_role("link", name=re.compile("검색")).first,
                 lambda: page.get_by_role("button", name=re.compile("검색")).first):
        try:
            make().click(timeout=4000)
            page.wait_for_url(re.compile(r"itgrSrch|search", re.I), timeout=8000)
            return
        except Exception:
            continue
    resp = page.goto(f"{BASE}/tme/itgrSrch", wait_until="domcontentloaded", timeout=NAV_TIMEOUT, referer=BASE)
    if resp is not None and resp.status in (403, 429):
        raise Blocked(f"검색 페이지에서 HTTP {resp.status} (메인은 통과)")


def open_via_search(page):
    if HUMAN_ENTRY:
        enter_search_like_human(page)
    else:
        stage("검색 페이지 접속")
        resp = page.goto(f"{BASE}/tme/itgrSrch", wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        if resp is not None and resp.status in (403, 429):
            raise Blocked(f"HTTP {resp.status}")
    box = page.locator("input[placeholder*='검색어'], input[type=search]").first
    try:
        box.wait_for(timeout=20000)
    except Exception:
        detect_block(page)
        raise Changed(f"검색창이 없음: {body_text(page)[:100]}")

    stage("작품 검색")
    box.fill(KEYWORD)
    box.press("Enter")
    try:
        page.get_by_text("예매하기", exact=True).first.wait_for(timeout=10000)
    except Exception:
        pass

    stage("예매하기 클릭")
    click_any("예매하기", [
        lambda: page.locator("li, div").filter(has_text=MOVIE_TITLE)
                    .filter(has=page.get_by_text("예매하기", exact=True)).last.get_by_text("예매하기", exact=True).first,
        lambda: page.get_by_text("예매하기", exact=True).first,
        lambda: page.get_by_role("button", name=re.compile("예매")).first,
    ], timeout=8000)
    try:
        page.wait_for_url(re.compile(r"movieBook"), timeout=20000)
        page.get_by_text(MOVIE_TITLE).first.wait_for(timeout=10000)
    except Exception:
        detect_block(page)
        raise Changed(f"예매 화면/작품({MOVIE_TITLE}) 확인 실패")
    set_state(book_url=page.url)


def open_fast(page):
    stage("예매 화면 새로고침")
    if "movieBook" in page.url:
        resp = page.reload(wait_until="domcontentloaded", timeout=30000)
    else:
        resp = page.goto(STATE["book_url"], wait_until="domcontentloaded", timeout=30000)
    if resp is not None and resp.status in (403, 429):
        raise Blocked(f"HTTP {resp.status}")
    try:
        page.get_by_text(MOVIE_TITLE).first.wait_for(timeout=12000)
    except Exception:
        detect_block(page)
        raise Changed("빠른 경로에서 작품을 못 찾음")


def ensure_theater(page):
    stage("극장 확인")
    try:
        page.wait_for_function(READY_JS, arg=THEATER, timeout=8000)
    except Exception:
        pass
    empty = page.get_by_text(re.compile("선택 된 극장이 없습니다")).first
    try:
        need_pick = empty.is_visible()
    except Exception:
        need_pick = False
    if not need_pick:
        need_pick = page.get_by_text(THEATER, exact=True).count() == 0
    if not need_pick:
        return

    stage("극장 선택")
    try:
        empty.click(timeout=8000)
    except Exception:
        raise Changed("극장 선택 버튼을 못 찾음")
    try:
        page.get_by_text("지역별", exact=True).first.wait_for(timeout=4000)
    except Exception:
        try:
            empty.locator("xpath=..").locator("button, [role=button]").last.click(timeout=5000)
            page.get_by_text("지역별", exact=True).first.wait_for(timeout=4000)
        except Exception:
            raise Changed("극장 목록이 안 열림")
    try:
        page.get_by_text(re.compile(r"^서울")).first.click(timeout=2000)
    except Exception:
        pass
    click_any(f"극장({THEATER})", [lambda: page.get_by_text(THEATER, exact=True).first], timeout=8000)
    click_any("극장선택 버튼", [lambda: page.get_by_text("극장선택", exact=True).first,
                              lambda: page.get_by_role("button", name=re.compile("극장선택")).first], timeout=8000)


def read_dates(page):
    stage("날짜 줄 읽기")
    try:
        page.wait_for_function(DATE_PRESENT_JS, timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(400)
    res = page.evaluate(DATE_JS)
    if not res:
        page.wait_for_timeout(2500)
        res = page.evaluate(DATE_JS)
    seen, rows = set(), []
    for r in res:
        if r["day"] not in seen:
            seen.add(r["day"])
            rows.append(r)
    if not rows:
        raise Changed("날짜 목록을 못 찾음")
    dates = real_dates([r["day"] for r in rows])
    status = " ".join(f"{d.day}{'✓' if r['enabled'] else '✗'}" for d, r in zip(dates, rows))
    enabled = [d for d, r in zip(dates, rows) if r["enabled"]]

    img = None
    try:
        xs0 = min(r["rect"]["x"] for r in rows)
        ys0 = min(r["rect"]["y"] for r in rows)
        xs1 = max(r["rect"]["x"] + r["rect"]["w"] for r in rows)
        ys1 = max(r["rect"]["y"] + r["rect"]["h"] for r in rows)
        x0, y0 = max(0, xs0 - 30), max(0, ys0 - 120)
        x1, y1 = min(1400, xs1 + 30), min(1000, ys1 + 420)
        if x1 - x0 > 50 and y1 - y0 > 50:
            img = snap(page, clip={"x": x0, "y": y0, "width": x1 - x0, "height": y1 - y0})
    except Exception:
        img = None
    if img is None:
        img = snap(page)
    return enabled, status, img


def check(page, force_full):
    if not force_full and STATE.get("book_url"):
        try:
            open_fast(page)
        except Exception as e:
            print("fast path failed → full path:", e)
            open_via_search(page)
    else:
        open_via_search(page)
    ensure_theater(page)
    return read_dates(page)


def one_check(browser, holder, attempts=(1, 2, 3)):
    """확인 1회. 실패하면 단계적으로 재시도 (빠른 확인 → 새 브라우저로 처음부터 → 잠깐 쉬고 한 번 더)"""
    last = None
    for attempt in attempts:
        try:
            if attempt > 1 or not holder.get("page") or holder["page"].is_closed():
                reset_browser(holder["browser"], holder)
                if attempt > 1:
                    time.sleep(3 * attempt)
            return check(holder["page"], force_full=(attempt > 1))
        except Blocked as e:
            holder["fail_shot"] = snap(holder["page"], full_page=False)
            holder["fail_stage"] = STAGE
            if attempt < (3 if e.soft else 2):
                last = e
                print(f"blocked? (try {attempt}, 단계: {STAGE}):", e)
                if not e.soft and PROXY_CONF and not holder.get("proxy_on") and len(attempts) > 1:
                    use_proxy(holder)
                continue
            raise
        except Exception as e:
            last = e
            print(f"error (try {attempt}, 단계: {STAGE}):", e)
            holder["fail_shot"] = snap(holder["page"], full_page=True) or snap(holder["page"])
            holder["fail_stage"] = STAGE
    raise last


# ───────── 알림 로직 ─────────
def alert_problem(kind, detail, holder):
    """문제 알림 → 로그 채널로, 서버마다 ERR_NOTICE_MIN분에 한 번만"""
    now = time.time()
    set_state(problem=True)
    if now - STATE.get("last_err_notice", 0) < ERR_NOTICE_MIN * 60:
        return
    set_state(last_err_notice=now)
    img = holder.get("fail_shot")
    if img:
        try:
            open("debug.jpg", "wb").write(img)
        except Exception:
            pass
    log(f"{kind} (단계: {holder.get('fail_stage') or STAGE})\n{detail[:300]}", img)


def on_success(enabled, status, img, streak):
    now = time.time()
    today = dt.datetime.now(KST).date()
    print(dt.datetime.now(KST).strftime("%H:%M:%S"), "status:", status, f"(연속 {streak}회)")

    if STATE.get("problem"):
        log(f"✅ 다시 정상적으로 확인되고 있어요 | 날짜: {status}")
        set_state(problem=False)
    set_state(last_ok=now, block_exits=0)

    # 예매 오픈 → 중요 알림 채널. 다시 닫힌 날짜는 기록에서 빼서, 진짜로 열릴 때 다시 알려 줌
    enabled_iso = {d.isoformat() for d in enabled}
    notified = {x for x in STATE.get("notified", []) if x in enabled_iso}
    hit = [d for d in enabled if d >= CUTOFF and d.isoformat() not in notified]
    if hit:
        txt = (f"{MENTION} " if MENTION else "") + f"🎬 {KEYWORD} {THEATER} 예매 오픈! " \
              + ", ".join(d.strftime("%m/%d(%a)") for d in hit) + f"\n{BASE}"
        alert(txt, img)
        notified |= {d.isoformat() for d in hit}
    set_state(notified=sorted(notified))

    # 기준일 전에 새로 열린 날짜는 참고용 → 로그 채널
    prev = {dt.date.fromisoformat(x) for x in STATE.get("enabled", [])}
    if "enabled" in STATE:
        fresh = [d for d in enabled if d not in prev and today <= d < CUTOFF]
        if fresh:
            log("🔔 새로 열린 날짜(기준일 전): " + ", ".join(d.strftime("%m/%d(%a)") for d in fresh))
    set_state(enabled=sorted(enabled_iso))

    near = [d for d in enabled if d >= CUTOFF]
    near_txt = ", ".join(d.strftime("%m/%d") for d in near) if near else "없음"
    send_status(f"📅 날짜 상태: {status}\n{CUTOFF} 이후 열림: {near_txt} | 연속 {streak}회 정상", img)

    if HEARTBEAT_MIN and now - STATE.get("last_hb", 0) >= HEARTBEAT_MIN * 60:
        log(f"🔄 정상 작동 중 — 연속 {streak}회 성공 | 날짜: {status} | {CUTOFF} 이후 열림: {near_txt}")
        set_state(last_hb=now)


def gap_watch():
    """오래 확인을 못 하고 있으면 알림 (무료 프록시 서버 D는 원래 불안정해서 로그 채널로만)"""
    now = time.time()
    gap = now - STATE.get("last_ok", now)
    if gap >= GAP_ALERT_MIN * 60 and now - STATE.get("last_gap_notice", 0) >= GAP_ALERT_MIN * 60:
        msg = (f"⏳ {int(gap // 60)}분째 CGV를 확인하지 못하고 있어요 (접속 제한 중일 수 있음). "
               "그동안 열린 날짜는 못 봤을 수 있습니다.")
        (log if PROXY_FREE else alert)(msg)
        set_state(last_gap_notice=now)


def sleep_until_slot():
    now = time.time()
    phase = (now - SLOT_OFFSET) % INTERVAL
    time.sleep(max(1, now - phase + INTERVAL - now))


def main():
    print(f"CGV 감시 시작 — 서버 {WORKER.upper() or '단독'}, 서버 목록 {ROSTER or '-'}, 간격 {INTERVAL}초, "
          f"{LOOP_MINUTES}분 동안, GitHub={ON_GITHUB}, 브라우저={os.getenv('BROWSER_CHANNEL') or '기본'}")
    if not check_webhook():
        if ON_GITHUB:
            time.sleep(300)   # 웹훅이 죽은 채로 재시작이 계속 반복되며 시간을 쓰지 않도록
        sys.exit(1)
    if SEND_START:
        log(f"✅ CGV 모니터링 시작: '{KEYWORD}' / {THEATER} / {CUTOFF} 이후 날짜 감시 ({INTERVAL // 60}분 간격)")
    if "last_ok" not in STATE:
        set_state(last_ok=time.time())
    blocked_streak, streak, give_up = 0, 0, False
    end = time.time() + LOOP_MINUTES * 60
    with sync_playwright() as p:
        holder = {"pw": p, "proxy_on": False, "proxy": None, "pool": [], "last_search": 0, "tested": 0}
        if PROXY_FREE:
            holder["browser"] = None
        else:
            holder["browser"] = launch_browser(p)
            reset_browser(holder["browser"], holder)
        print("브라우저 준비 완료 — 첫 확인을 시작합니다")
        while time.time() < end and not give_up:
            t0, pause = time.time(), None
            holder["fail_shot"] = None
            try:
                if PROXY_FREE:
                    enabled, status, img = one_check_free(holder)
                else:
                    enabled, status, img = one_check(holder["browser"], holder)
                blocked_streak = 0
                streak += 1
                on_success(enabled, status, img, streak)
            except NoProxy as e:
                streak = 0
                print("proxy:", e)
                alert_problem("🔎 프록시 없음", str(e), holder)
                gap_watch()
            except Blocked as e:
                streak = 0
                blocked_streak += 1
                print("blocked:", e)
                if not PROXY_FREE:
                    give_up = ON_GITHUB and blocked_streak >= MAX_BLOCKED
                    if give_up:
                        set_state(block_exits=STATE.get("block_exits", 0) + 1)
                    else:   # 바로 다시 두드리지 않고 점점 늘려서 기다림 (2분 → 4분 → 8분 ... 최대 15분)
                        pause = min(120 * 2 ** (blocked_streak - 1), 900)
                alert_problem("🚫 CGV가 접속을 막았어요" + (" → 새 서버로 갈아탑니다" if give_up else
                              f" → {pause // 60}분 쉬고 다시 시도" if pause else ""), str(e), holder)
                gap_watch()
            except Exception as e:
                streak = 0
                print("error:", e)
                alert_problem("⚠️ 확인에 실패했어요 (자동 재시도 모두 실패)", str(e), holder)
                gap_watch()
            print(f"  이번 확인 소요 {time.time() - t0:.1f}초")
            if give_up:
                break
            if pause:
                time.sleep(min(pause, max(0, end - time.time())))
            else:
                sleep_until_slot()
        for k in ("page", "ctx", "browser"):
            try:
                holder[k].close()
            except Exception:
                pass
    if give_up:
        n = STATE.get("block_exits", 1)
        time.sleep(min(BLOCK_BACKOFF * n, 300))   # 차단→재시작이 연속될수록 조금씩 더 쉼


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print("💥 치명적 오류:", repr(e))
        alert(f"💥 봇이 중간에 멈췄어요: {str(e)[:300]}")
        if ON_GITHUB:
            time.sleep(60)
        raise
