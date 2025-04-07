# -*- coding: utf-8 -*-
"""fashionScrape.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1p1fu3AL_qNGnDgKWg72ia9j7rWt1eNdQ
"""

!pip install playwright
!playwright install

import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import pandas as pd

# Allow asyncio to run in Colab
nest_asyncio.apply()

async def scrape_redisvss():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        await page.goto("https://redisvss.partee.io/", timeout=60000)

        # Wait for elements to load
        await page.wait_for_selector(".card-text")

        unique_products = set()
        max_attempts = 500  # Adjust based on how many cycles you want to try
        consecutive_repeats = 0  # Track how often we see mostly the same products
        last_batch = set()

        for _ in range(max_attempts):
            # Extract product names
            product_names = await page.eval_on_selector_all(".card-text", "elements => elements.map(el => el.innerText)")
            product_set = set(product_names)

            # Check if we are seeing mostly repeated products
            if product_set == last_batch:
                consecutive_repeats += 1
            else:
                consecutive_repeats = 0  # Reset if we get new products

            unique_products.update(product_set)
            last_batch = product_set  # Store last seen products

            if consecutive_repeats >= 3:  # Stop if we see mostly repeated data for 3 cycles
                print("Detected repeated products. Stopping.")
                break

            # Click "Load More Products" and wait
            try:
                load_more_button = await page.wait_for_selector("text=Load More Products", timeout=5000)
                await load_more_button.click()
                await page.wait_for_timeout(3000)  # Wait for new products to appear
            except:
                print("No more 'Load More Products' button found.")
                break

        # Save to CSV
        df = pd.DataFrame(list(unique_products), columns=["Product Name"])
        df.to_csv("products.csv", index=False)

        await browser.close()
        print(f"Scraping done. Collected {len(unique_products)} unique products. File saved as 'products.csv'.")

# Run the async function manually
loop = asyncio.get_event_loop()
loop.run_until_complete(scrape_redisvss())