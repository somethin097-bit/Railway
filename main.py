import asyncio, re, time, os, json, threading
from curl_cffi import requests as cf_requests
from playwright.async_api import async_playwright
from flask import Flask

# ============ FLASK KEEP-ALIVE (runs in background) ============
app = Flask(__name__)

@app.route("/")
def home():
    return "alive"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask, daemon=True).start()

# ============ CONFIG ============
VIDEO_URL      = "https://rumble.com/v73ya64-25-missing-kids-discovered-behind-secret-door.html"
UA             = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
PARALLEL_VIEWS = 10
TOTAL_VIEWS    = 1000000
WATCH_SECONDS  = 10

# ============ FETCH PAGE ONCE ============
print("🔄 Fetching page...")
session = cf_requests.Session(impersonate="chrome120")
headers = {
    'User-Agent': UA,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://rumble.com/',
}
session.get("https://rumble.com", headers=headers)
html = session.get(VIDEO_URL, headers=headers).text
print(f"✅ {len(html):,} chars")

embeds = re.findall(r'https://rumble\.com/embed/[a-zA-Z0-9]+', html)
embed_url = embeds[0] if embeds else None
print(f"✅ Embed: {embed_url}")

fixed = html
for o, n in [
    ('href="/', 'href="https://rumble.com/'),
    ("href='/", "href='https://rumble.com/"),
    ('src="/',  'src="https://rumble.com/'),
    ("src='/",  "src='https://rumble.com/"),
]:
    fixed = fixed.replace(o, n)

with open('/tmp/page.html', 'w', encoding='utf-8') as f:
    f.write(fixed)
print("✅ Page saved!")

# ============ SHARED RESULTS TRACKER ============
all_results = []
view_counter = {'count': 0}

# ============ SINGLE VIEW FUNCTION ============
async def run_single_view(view_num, semaphore):
    async with semaphore:
        result = {
            'view_num': view_num,
            'view_fired': False,
            'preroll_ad': False,
            'pause_ad': False,
            'ad_creative': False,
            'ads': 0,
            'video_played': False,
            'max_time': 0,
            'status': 'running',
        }

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--single-process',
                        '--no-zygote',
                        '--use-gl=swiftshader',
                        '--autoplay-policy=no-user-gesture-required',
                        '--disable-blink-features=AutomationControlled',
                    ]
                )

                context = await browser.new_context(
                    user_agent=UA,
                    viewport={'width': 1440, 'height': 900},
                    locale='en-US',
                    timezone_id='America/New_York',
                )

                await context.add_init_script("""
                    Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                    Object.defineProperty(navigator,'hardwareConcurrency',{get:()=>8});
                    Object.defineProperty(navigator,'deviceMemory',{get:()=>8});
                    Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
                    Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
                    window.chrome={runtime:{},loadTimes:()=>{},csi:()=>{},app:{}};
                """)

                page = await context.new_page()

                # Track
                async def on_response(response):
                    url = response.url
                    if 'rumble.com/l/view' in url:
                        result['view_fired'] = True
                    if any(x in url.lower() for x in ['vast', 'preroll', 'ad.xml']):
                        result['preroll_ad'] = True
                        try:
                            body = await response.text()
                            result['vast_content'] = body[:300]
                        except:
                            pass
                    if 'pausejs' in url:
                        result['pause_ad'] = True
                    if any(x in url for x in ['googleads.g.doubleclick', '2mdn.net/videoplayback']):
                        result['ad_creative'] = True
                    if any(x in url for x in ['imasdk', 'googlesyndication', 'doubleclick', 'ads.rmbl', '2mdn']):
                        result['ads'] += 1

                page.on('response', on_response)

                # Load
                await page.goto('file:///tmp/page.html',
                                wait_until='domcontentloaded',
                                timeout=30000)
                await asyncio.sleep(2)

                # Inject embed
                await page.evaluate(f"""
                    () => {{
                        document.querySelectorAll('iframe').forEach(f=>f.remove());
                        const i = document.createElement('iframe');
                        i.src = '{embed_url}?autoplay=2&pub=0';
                        i.style.cssText = 'position:fixed;top:0;left:0;width:1440px;height:900px;z-index:99999;border:none;background:#000';
                        i.allow = 'autoplay; fullscreen; encrypted-media';
                        document.body.appendChild(i);
                    }}
                """)

                await asyncio.sleep(6)

                # Find frame
                rumble_frame = None
                for frame in page.frames:
                    if 'rumble.com/embed' in frame.url:
                        rumble_frame = frame
                        break

                if rumble_frame:
                    # Click play
                    try:
                        btn = await rumble_frame.wait_for_selector(
                            '[class*="bigPlay"]',
                            timeout=5000, state='visible'
                        )
                        await btn.click()
                    except:
                        await page.mouse.click(720, 450)

                    await asyncio.sleep(3)

                    # Watch
                    for i in range(WATCH_SECONDS // 10):
                        await asyncio.sleep(10)
                        try:
                            state = await rumble_frame.evaluate("""
                                () => {
                                    const v = document.querySelector('video');
                                    return v ? {
                                        time: Math.round(v.currentTime),
                                        paused: v.paused,
                                    } : null;
                                }
                            """)
                            if state and state.get('time', 0) > 0:
                                result['video_played'] = True
                                result['max_time'] = max(
                                    result['max_time'],
                                    state.get('time', 0)
                                )
                        except:
                            pass

                    # Pause
                    try:
                        await rumble_frame.evaluate("""
                            () => { document.querySelector('video')?.pause(); }
                        """)
                        await asyncio.sleep(3)
                    except:
                        pass

                await browser.close()
                result['status'] = 'done'

        except Exception as e:
            result['status'] = f'error: {str(e)[:50]}'

        view_counter['count'] += 1
        all_results.append(result)
        print(f"  ✅ View {view_num} done: "
              f"👁{'✅' if result['view_fired'] else '❌'} "
              f"📢{'✅' if result['preroll_ad'] else '❌'} "
              f"⏸{'✅' if result['pause_ad'] else '❌'} "
              f"▶️{'✅' if result['video_played'] else '❌'} "
              f"t={result['max_time']}s "
              f"ads={result['ads']}")
        return result

# ============ LIVE DASHBOARD ============
def print_dashboard(results, total):
    print(f"\n{'='*60}")
    print(f"📊 LIVE DASHBOARD - {len(results)}/{total} complete")
    print(f"{'='*60}")
    print(f"{'#':<4} {'View':<6} {'Preroll':<9} {'Pause':<7} {'Creative':<10} {'Played':<8} {'MaxT':<6} {'Ads'}")
    print(f"{'-'*60}")
    for r in sorted(results, key=lambda x: x['view_num']):
        print(
            f"{r['view_num']:<4} "
            f"{'✅' if r['view_fired'] else '❌':<6} "
            f"{'✅' if r['preroll_ad'] else '❌':<9} "
            f"{'✅' if r['pause_ad'] else '❌':<7} "
            f"{'✅' if r['ad_creative'] else '❌':<10} "
            f"{'✅' if r['video_played'] else '❌':<8} "
            f"{r['max_time']:<6} "
            f"{r['ads']}"
        )
    print(f"{'-'*60}")
    if results:
        views    = sum(1 for r in results if r['view_fired'])
        prerolls = sum(1 for r in results if r['preroll_ad'])
        pauses   = sum(1 for r in results if r['pause_ad'])
        played   = sum(1 for r in results if r['video_played'])
        n = len(results)
        print(f"TOTALS: 👁{views}/{n} 📢{prerolls}/{n} ⏸{pauses}/{n} ▶️{played}/{n}")
        print(f"Fill rate: {(prerolls/n)*100:.0f}% preroll | {(pauses/n)*100:.0f}% pause")

# ============ RUN PARALLEL BATCHES ============
async def run_all():
    semaphore = asyncio.Semaphore(PARALLEL_VIEWS)

    print(f"\n🚀 Starting {TOTAL_VIEWS} views ({PARALLEL_VIEWS} parallel)...")
    print(f"⏱ Est time: {(TOTAL_VIEWS/PARALLEL_VIEWS) * (WATCH_SECONDS+15):.0f}s\n")

    tasks = [
        run_single_view(i, semaphore)
        for i in range(1, TOTAL_VIEWS + 1)
    ]

    for coro in asyncio.as_completed(tasks):
        await coro
        print_dashboard(all_results, TOTAL_VIEWS)

    # Save report
    clean = [{k: v for k, v in r.items() if k != 'vast_content'}
             for r in all_results]
    with open('/tmp/parallel_report.json', 'w') as f:
        json.dump(clean, f, indent=2)
    print("✅ Report saved to /tmp/parallel_report.json")

    return all_results

if __name__ == "__main__":
    asyncio.run(run_all())
