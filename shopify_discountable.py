import os
import time
import requests

SHOP_URL = os.environ["SHOP_URL"]            # π.χ. your-store.myshopify.com (χωρίς https://)
ACCESS_TOKEN = os.environ["SHOPIFY_TOKEN"]   # Admin API access token (secret)
API_VERSION = os.environ.get("API_VERSION", "2025-01")

SESSION = requests.Session()
SESSION.headers.update({
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
})

TAG_NAME = os.environ.get("TAG_NAME", "discountable")
PAGE_LIMIT = 250
BASE_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}"

def shopify_get(url, params=None, max_retries=5):
    retries = 0
    while True:
        r = SESSION.get(url, params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2"))
            time.sleep(wait)
            retries += 1
            if retries > max_retries:
                r.raise_for_status()
            continue
        r.raise_for_status()
        return r

def shopify_put(url, json, max_retries=5):
    retries = 0
    while True:
        r = SESSION.put(url, json=json)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2"))
            time.sleep(wait)
            retries += 1
            if retries > max_retries:
                r.raise_for_status()
            continue
        r.raise_for_status()
        return r

def iter_products():
    endpoint = f"{BASE_URL}/products.json"
    params = {"limit": PAGE_LIMIT, "fields": "id,title,tags,variants"}
    next_url = endpoint

    while True:
        r = shopify_get(next_url, params=params)
        data = r.json()
        products = data.get("products", [])
        for p in products:
            yield p

        link = r.headers.get("Link", "")
        if 'rel="next"' not in link:
            break

        parts = [p.strip() for p in link.split(",")]
        next_link = None
        for p in parts:
            if 'rel="next"' in p:
                start = p.find("<") + 1
                end = p.find(">")
                next_link = p[start:end]
                break
        if not next_link:
            break

        next_url = next_link
        params = None

def product_has_any_compare_at(product):
    for v in product.get("variants", []):
        cap = v.get("compare_at_price")
        if cap is not None and str(cap).strip() != "":
            return True
    return False

def normalize_tags(tag_string):
    if not tag_string:
        return []
    tags = [t.strip() for t in tag_string.split(",") if t.strip()]
    seen = set()
    out = []
    for t in tags:
        low = t.lower()
        if low not in seen:
            seen.add(low)
            out.append(t)
    return out

def set_product_tags(product_id, tags_list):
    tag_str = ", ".join(tags_list)
    url = f"{BASE_URL}/products/{product_id}.json"
    payload = {"product": {"id": product_id, "tags": tag_str}}
    shopify_put(url, json=payload)

def main():
    updated_add = 0
    updated_remove = 0

    for product in iter_products():
        tags = normalize_tags(product.get("tags", ""))
        has_compare = product_has_any_compare_at(product)
        has_tag = any(t.lower() == TAG_NAME.lower() for t in tags)
        want_tag = not has_compare

        if want_tag and not has_tag:
            tags.append(TAG_NAME)
            set_product_tags(product["id"], tags)
            updated_add += 1
            print(f"✅ Added '{TAG_NAME}' → {product.get('title')} (#{product['id']})")

        elif not want_tag and has_tag:
            tags = [t for t in tags if t.lower() != TAG_NAME.lower()]
            set_product_tags(product["id"], tags)
            updated_remove += 1
            print(f"❌ Removed '{TAG_NAME}' → {product.get('title')} (#{product['id']})")

    print(f"\nDone. Added: {updated_add}, Removed: {updated_remove}")

if __name__ == "__main__":
    if not SHOP_URL or not ACCESS_TOKEN:
        raise SystemExit("Missing SHOP_URL or SHOPIFY_TOKEN environment variables.")
    main()
