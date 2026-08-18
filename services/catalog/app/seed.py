"""Demo catalogue data.

    docker compose exec catalog-api python -m app.seed

Idempotent: matches on slug and SKU, so re-running updates rather than
duplicating.

Two things beyond writing rows:

  - enqueues `catalog.product.upserted` for every product, so the Order service
    populates its price read-model. Without this, checkout rejects every item
    with "product not available" because Order has never heard of it.
  - `cached_stock` here is only Catalog's display copy. Inventory owns real stock
    and is seeded separately by `services/inventory/app/seed.py`.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

from common.db.outbox import enqueue_event
from common.db.session import Database
from common.events.envelope import make_envelope
from common.events.schemas import ProductUpserted
from common.events.topics import Topics
from common.observability.logging import configure_logging
from app.config import settings
from app.models import Outbox, Product
from app.repositories import CategoryRepository, ProductRepository

configure_logging(service_name="catalog-seed", level=settings.log_level, json_output=False)
logger = logging.getLogger(__name__)

CATEGORIES = [
    ("Audio", "audio", "Headphones, earbuds and speakers"),
    ("Computing", "computing", "Laptops, keyboards and mice"),
    ("Mobiles", "mobiles", "Phones and accessories"),
    ("Wearables", "wearables", "Watches and fitness bands"),
]

# (sku, name, category_slug, price, stock, description)
PRODUCTS = [
    ("AUD-SONY-XM5", "Sony WH-1000XM5", "audio", "26990.00", 25,
     "Industry-leading noise cancelling over-ear headphones."),
    ("AUD-BOSE-QC45", "Bose QuietComfort 45", "audio", "24900.00", 12,
     "Comfortable ANC headphones with balanced sound."),
    ("AUD-APPL-APP2", "Apple AirPods Pro (2nd gen)", "audio", "21900.00", 40,
     "Active noise cancellation with adaptive transparency."),
    ("AUD-JBL-FLIP6", "JBL Flip 6", "audio", "8999.00", 60,
     "Portable waterproof bluetooth speaker."),
    ("AUD-SENN-HD650", "Sennheiser HD 650", "audio", "34990.00", 4,
     "Open-back reference headphones for critical listening."),

    ("CMP-LOGI-MXM3", "Logitech MX Master 3S", "computing", "9495.00", 30,
     "Precision wireless mouse with quiet clicks."),
    ("CMP-KEYC-K2", "Keychron K2 V2", "computing", "7999.00", 18,
     "75% hot-swappable mechanical keyboard."),
    ("CMP-DELL-U2723", "Dell UltraSharp U2723QE", "computing", "58900.00", 6,
     "27-inch 4K IPS Black monitor with USB-C hub."),
    ("CMP-APPL-MBA13", "Apple MacBook Air 13 M3", "computing", "114900.00", 8,
     "Fanless laptop with the M3 chip."),
    ("CMP-SAMS-T7-1TB", "Samsung T7 Portable SSD 1TB", "computing", "8499.00", 45,
     "USB 3.2 external SSD, up to 1050 MB/s."),

    ("MOB-APPL-IP15", "Apple iPhone 15", "mobiles", "79900.00", 15,
     "6.1-inch Super Retina XDR with Dynamic Island."),
    ("MOB-SAMS-S24", "Samsung Galaxy S24", "mobiles", "74999.00", 20,
     "Compact flagship with Snapdragon 8 Gen 3."),
    ("MOB-GOOG-P8A", "Google Pixel 8a", "mobiles", "52999.00", 22,
     "Tensor G3 with seven years of updates."),
    ("MOB-ONEP-12R", "OnePlus 12R", "mobiles", "39999.00", 35,
     "120Hz display and 100W fast charging."),
    ("MOB-ANKR-737", "Anker 737 Power Bank", "mobiles", "12999.00", 50,
     "24000mAh, 140W bidirectional charging."),

    ("WER-APPL-AW9", "Apple Watch Series 9", "wearables", "41900.00", 14,
     "Double tap gesture and a brighter display."),
    ("WER-GARM-F7", "Garmin Forerunner 265", "wearables", "51990.00", 7,
     "AMOLED running watch with training readiness."),
    ("WER-SAMS-GW6", "Samsung Galaxy Watch 6", "wearables", "29999.00", 16,
     "Sleep coaching and body composition."),
    ("WER-FITB-CH6", "Fitbit Charge 6", "wearables", "14999.00", 3,
     "Heart rate tracking with Google apps built in."),
    ("WER-AMZF-GTR4", "Amazfit GTR 4", "wearables", "17999.00", 0,
     "Two-week battery life. Currently out of stock."),
]


async def seed() -> None:
    db = Database(settings.database_url, pool_size=5, max_overflow=0)

    async with db.transaction() as session:
        categories = CategoryRepository(session)
        products = ProductRepository(session)

        slug_to_id = {}
        for name, slug, description in CATEGORIES:
            existing = await categories.get_by_slug(slug)
            if existing is None:
                created = await categories.create(name=name, slug=slug, description=description)
                slug_to_id[slug] = created.id
                logger.info("category created: %s", slug)
            else:
                slug_to_id[slug] = existing.id

        created_count = updated_count = 0
        for sku, name, category_slug, price, stock, description in PRODUCTS:
            existing = await products.get_by_sku(sku)
            if existing is None:
                product = await products.create(
                    sku=sku,
                    name=name,
                    description=description,
                    price=Decimal(price),
                    currency="INR",
                    category_id=slug_to_id[category_slug],
                    cached_stock=stock,
                )
                created_count += 1
            else:
                product = await products.apply_updates(
                    existing,
                    {
                        "name": name,
                        "description": description,
                        "price": Decimal(price),
                        "category_id": slug_to_id[category_slug],
                        "cached_stock": stock,
                        "is_active": True,
                    },
                )
                updated_count += 1

            # Order needs this to price a checkout. Enqueued in the same
            # transaction as the row, then relayed by catalog-outbox.
            await _enqueue_upsert(session, product)

        logger.info(
            "seed complete: %d products created, %d updated, %d categories, "
            "%d product.upserted events queued",
            created_count,
            updated_count,
            len(CATEGORIES),
            created_count + updated_count,
        )


async def _enqueue_upsert(session, product: Product) -> None:
    envelope = make_envelope(
        event_type=Topics.CATALOG_PRODUCT_UPSERTED,
        payload=ProductUpserted(
            product_id=product.id,
            sku=product.sku,
            name=product.name,
            price=product.price,
            currency=product.currency,
            is_active=product.is_active,
        ),
        producer="catalog-seed",
    )
    await enqueue_event(
        session,
        Outbox,
        topic=Topics.CATALOG_PRODUCT_UPSERTED,
        key=str(product.id),
        envelope=envelope,
        aggregate_id=product.id,
    )

    await db.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
