import os
import time
import requests

SHOP_URL = os.environ["SHOP_URL"]            # π.χ. your-store.myshopify.com (χωρίς https://)
ACCESS_TOKEN = os.environ["SHOPIFY_TOKEN"]   # Admin API access token (secret)
API_VERSION = os.environ.get("API_VERSION", "2025-01")

# Δώσε ΕΝΑ από τα δύο:
COLLECTION_ID = os.environ.get("537605406986")          # π.χ. 1234567890 (προτείνεται αν το ξέρεις)
COLLECTION_TITLE = os.environ.get("Compare price", "compare price")  # εναλλακτικά, ακριβής τίτλος

TAG_NAME = os.environ.get("TAG_NAME", "discountable")
PAGE_LIMIT = 250
BASE_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}"

SESSION = requests.Session()
SESSION.headers.update({
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
})

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

def find_collection_id_by_title(title):
    """
    Αναζητά collection με exact title σε Smart Collections ΚΑΙ Custom Collections.
    Επιστρέφει το πρώτο ακριβές match (κατά προτίμηση Smart), αλλιώς None.
    """
    title_l = title.strip().lower()

    # 1) Smart collections
    next_url = f"{BASE_URL}/smart_collections.json"
    params = {"limit": PAGE_LIMIT, "fields": "id,title"}
    while True:
        r = shopify_get(next_url, params=params)
        data = r.json().get("smart_collections", [])
        for c in data:
            if str(c.get("title", "")).strip().lower() == title_l:
                return str(c["id"])
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link: break
        next_url, params = extract_next_link(link), None

    # 2) Custom collections (αν δεν βρέθηκε στα smart)
    next_url = f"{BASE_URL}/custom_collections.json"
    params = {"limit": PAGE_LIMIT, "fields": "id,title"}
    while True:
        r = shopify_get(next_url, params=params)
        data = r.json().get("custom_collections", [])
        for c in data:
            if str(c.get("title", "")).strip().lower() == title_l:
                return str(c["id"])
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link: break
        next_url, params = extract_next_link(link), None

    return None

def extract_next_link(link_header):
    # Παίρνει το URL της επόμενης σελίδας από το Link header
    parts = [p.strip() for p in link_header.split(",")]
    for p in parts:
        if 'rel="next"' in p:
            start = p.find("<") + 1
            end = p.find(">")
            return p[start:end]
    return None

def get_product_ids_in_collection(collection_id):
    """
    Επιστρέφει set με ΟΛΑ τα product IDs που ανήκουν στη συλλογή (smart ή custom)
    χρησιμοποιώντας /products.json?collection_id=
    """
    product_ids = set()
    next_url = f"{BASE_URL}/products.json"
    params = {"limit": PAGE_LIMIT, "fields": "id", "collection_id": collection_id}

    while True:
        r = shopify_get(next_url, params=params)
        products = r.json().get("products", [])
        for p in products:
            product_ids.add(p["id"])
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link: break
        next_url, params = extract_next_link(link), None

    return product_ids

def iter_all_products():
    next_url = f"{BASE_URL}/products.json"
    params = {"limit": PAGE_LIMIT, "fields": "id,title,tags"}
    while True:
        r = shopify_get(next_url, params=params)
        data = r.json().get("products", [])
        for p in data:
            yield p
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link: break
        next_url, params = extract_next_link(link), None

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
    # Βρες collection_id
    collection_id = COLLECTION_ID
    if not collection_id:
        if not COLLECTION_TITLE:
            raise SystemExit("Πρέπει να ορίσεις COLLECTION_ID ή COLLECTION_TITLE.")
        collection_id = find_collection_id_by_title(COLLECTION_TITLE)
        if not collection_id:
            raise SystemExit(f"Δε βρέθηκε collection με τίτλο: {COLLECTION_TITLE}")

    print(f"ℹ️ Χρησιμοποιώ collection_id={collection_id}")

    # Product IDs που ΑΝΗΚΟΥΝ στη compare price collection
    in_collection = get_product_ids_in_collection(collection_id)
    print(f"ℹ️ Προϊόντα στη συλλογή: {len(in_collection)}")

    updated_add = 0
    updated_remove = 0

    # Πέρασε από ΟΛΑ τα προϊόντα
    for product in iter_all_products():
        pid = product["id"]
        title = product.get("title")
        tags = normalize_tags(product.get("tags", ""))

        # Αν είναι στη collection (έχει compare) → ΔΕ θέλουμε tag
        want_tag = (pid not in in_collection)
        has_tag = any(t.lower() == TAG_NAME.lower() for t in tags)

        if want_tag and not has_tag:
            tags.append(TAG_NAME)
            set_product_tags(pid, tags)
            updated_add += 1
            print(f"✅ Added '{TAG_NAME}' → {title} (#{pid})")

        elif not want_tag and has_tag:
            tags = [t for t in tags if t.lower() != TAG_NAME.lower()]
            set_product_tags(pid, tags)
            updated_remove += 1
            print(f"❌ Removed '{TAG_NAME}' → {title} (#{pid})")

    print(f"\nDone. Added: {updated_add}, Removed: {updated_remove}")

if __name__ == "__main__":
    if not SHOP_URL or not ACCESS_TOKEN:
        raise SystemExit("Missing SHOP_URL or SHOPIFY_TOKEN environment variables.")
    main()
