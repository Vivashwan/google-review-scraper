import asyncio
from playwright.async_api import async_playwright

URL = "https://www.google.com/maps/place/Paradise+Biryani+%7C+Secunderabad/@17.4417141,78.4872154,17z/data=!4m7!3m6!1s0x3bcb9a0f8cf1fd1b:0x386e919f25da1d16!8m2!3d17.4417141!4d78.4872154?hl=en&gl=in"

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox","--disable-gpu","--ozone-platform=x11"]
        )
        page = await browser.new_page()
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(4)

        btn = await page.wait_for_selector('button[aria-label*="Reviews"]', timeout=10000)
        await btn.click()
        await asyncio.sleep(4)

        try:
            sort = await page.wait_for_selector('button[aria-label*="Sort reviews"]', timeout=5000)
            await sort.click()
            await asyncio.sleep(1)
            newest = await page.wait_for_selector('[data-index="1"]', timeout=3000)
            await newest.click()
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Sort failed: {e}")

        container = None
        for sel in ['div[aria-label*="Reviews"][role="feed"]','div[role="feed"]','.m6QErb.DxyBCb','.m6QErb']:
            try:
                container = await page.wait_for_selector(sel, timeout=8000, state="attached")
                if container:
                    print(f"Container: {sel}")
                    break
            except:
                continue

        # Scroll in batches and check after each batch
        TARGETS = ['Anoop Goyal', 'Sundeep Kumar', 'SHYAM SUNDER', 'S NAYEEMUDDIN', 'Nagaraj']

        for batch in range(20):  # 20 batches of 10 scrolls = 200 total scrolls
            for _ in range(10):
                await page.evaluate(
                    "(el) => el.scrollBy({top: 600, behavior: 'instant'})", container
                )
                await asyncio.sleep(0.2)
            await asyncio.sleep(1)

            # Check if targets are visible
            found = await page.evaluate("""
            (targets) => {
                const allEls = document.querySelectorAll('[data-review-id]');
                const blocks = [];
                allEls.forEach(el => {
                    const parent = el.parentElement
                        ? el.parentElement.closest('[data-review-id]')
                        : null;
                    if (!parent) blocks.push(el);
                });
                const names = [];
                blocks.forEach(block => {
                    const nameEl = block.querySelector('.d4r55');
                    if (nameEl) names.push(nameEl.innerText.trim());
                });
                const foundTargets = targets.filter(t =>
                    names.some(n => n.includes(t))
                );
                return { total_visible: names.length, found_targets: foundTargets, last_5: names.slice(-5) };
            }
            """, TARGETS)

            print(f"Batch {batch+1}: {found['total_visible']} reviews visible | found: {found['found_targets']} | last 5: {found['last_5']}")

            if found['found_targets']:
                print("TARGETS FOUND — extracting details...")
                break

        # Now extract details for targets
        result = await page.evaluate(r"""
        () => {
            const allEls = document.querySelectorAll('[data-review-id]');
            const blocks = [];
            allEls.forEach(el => {
                const parent = el.parentElement
                    ? el.parentElement.closest('[data-review-id]')
                    : null;
                if (!parent) blocks.push(el);
            });

            const TARGETS = ['Anoop Goyal', 'Sundeep Kumar', 'SHYAM SUNDER', 'S NAYEEMUDDIN', 'Nagaraj'];
            const output = [];

            blocks.forEach(block => {
                const nameEl = block.querySelector('.d4r55');
                const name = nameEl ? nameEl.innerText.trim() : '?';
                if (!TARGETS.some(t => name.includes(t))) return;

                const buttons = block.querySelectorAll('button.w8nwRe');
                const btnDetails = [];
                buttons.forEach(btn => {
                    const controlsId = btn.getAttribute('aria-controls');
                    let controlledText = null;
                    let controlledHidden = null;
                    if (controlsId) {
                        const el = document.getElementById(controlsId);
                        if (el) {
                            controlledText   = (el.innerText || '').substring(0, 120);
                            controlledHidden = el.getAttribute('aria-hidden');
                        }
                    }
                    btnDetails.push({
                        ariaLabel:       btn.getAttribute('aria-label') || '',
                        ariaExpanded:    btn.getAttribute('aria-expanded') || '',
                        jsaction:        (btn.getAttribute('jsaction') || '').substring(0, 80),
                        controlledText,
                        controlledHidden,
                    });
                });

                const replyEl = block.querySelector('.CDe7pd');
                output.push({
                    name,
                    buttons: btnDetails,
                    owner_reply_text: replyEl
                        ? (replyEl.innerText || '').substring(0, 200)
                        : null,
                });
            });
            return output;
        }
        """)

        import json
        print(json.dumps(result, indent=2, ensure_ascii=False))
        await asyncio.sleep(5)
        await browser.close()

asyncio.run(debug())
