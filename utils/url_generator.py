"""Deterministic fake URL and page-content generation.

Exposes:
    DOMAIN_CATEGORIES, fake_page_content, generate_url_pool,
    and get_domain_category.

Design notes:
    Generated pages are synthetic and deterministic. No real websites are
    contacted, which keeps demos repeatable and safe to run offline.
"""


import hashlib
import random
from typing import Any

DOMAIN_CATEGORIES = {
    "sports": [
        "espn.sports.com", "bleacherreport.sports.net", "sportnews.io",
        "athleticscore.com", "gamezone.sports.org",
    ],
    "tech": [
        "techcrunch.tech.com", "hackernews.tech.io", "devblog.tech.net",
        "silicon.tech.org", "codedaily.tech.com",
    ],
    "news": [
        "globalnews.news.com", "dailyreport.news.net", "liveheadlines.news.io",
        "pressroom.news.org", "flashnews.news.com",
    ],
    "science": [
        "naturemag.science.com", "arxivdaily.science.net", "labresults.science.io",
        "researchhub.science.org", "peerreview.science.com",
    ],
    "finance": [
        "stockwatch.finance.com", "marketpulse.finance.net", "traderdesk.finance.io",
        "cryptofeed.finance.org", "wallstreetbuzz.finance.com",
    ],
}

PATH_TEMPLATES = [
    "/article/{id}",
    "/news/{year}/{month}/{slug}",
    "/post/{id}/details",
    "/story/{slug}-{id}",
    "/category/{cat}/item/{id}",
    "/p/{id}",
    "/report/{year}-{id}",
    "/live/{id}/updates",
]

WORDS = [
    "breaking", "analysis", "review", "update", "insight",
    "deep-dive", "summary", "exclusive", "trending", "top",
    "weekly", "daily", "special", "featured", "latest",
]


def _make_url(domain: str, idx: int) -> str:
    """Build a fake URL from a domain and an index."""
    template = PATH_TEMPLATES[idx % len(PATH_TEMPLATES)]
    slug = WORDS[idx % len(WORDS)]
    year = 2023 + (idx % 3)
    month = str((idx % 12) + 1).zfill(2)
    path = template.format(id=idx, slug=slug, year=year, month=month, cat="main")
    return f"https://{domain}{path}"


def fake_page_content(url: str) -> dict[str, Any]:
    """
    Deterministic fake page data seeded by URL hash.
    Same URL always returns the same fake response — fair for benchmarking.
    """
    seed = int(hashlib.md5(url.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    word_count = rng.randint(200, 2000)
    alpha_ratio = rng.uniform(0.60, 0.95)
    sentence_count = rng.randint(5, 50)
    quality = round(alpha_ratio * 0.5 + min(sentence_count / 50, 1.0) * 0.5, 3)
    latency_ms = rng.randint(50, 800)
    return {
        "url": url,
        "word_count": word_count,
        "alpha_ratio": alpha_ratio,
        "sentence_count": sentence_count,
        "quality_score": quality,
        "simulated_latency_ms": latency_ms,
        "domain": url.split("/")[2],
    }


def generate_url_pool(
    total: int = 20000, duplicate_rate: float = 0.10
) -> tuple[list[str], dict[str, str]]:
    """
    Returns:
      url_pool  — list of `total` URLs (includes intentional duplicates)
      domain_map — {url: category} for all unique URLs
    """
    unique_target = int(total * (1 - duplicate_rate))
    rng = random.Random(42)   # fixed seed → reproducible pool

    urls: list[str] = []
    domain_map: dict[str, str] = {}

    # Generate unique URLs
    per_domain_count = unique_target // (len(DOMAIN_CATEGORIES) * 5)
    for cat, domains in DOMAIN_CATEGORIES.items():
        for domain in domains:
            for i in range(per_domain_count):
                idx = len(urls)
                url = _make_url(domain, idx)
                urls.append(url)
                domain_map[url] = cat

    # Pad to unique_target if we're short
    while len(urls) < unique_target:
        cat = rng.choice(list(DOMAIN_CATEGORIES.keys()))
        domain = rng.choice(DOMAIN_CATEGORIES[cat])
        url = _make_url(domain, len(urls) + 99999)
        if url not in domain_map:
            urls.append(url)
            domain_map[url] = cat

    unique_urls = list(urls[:unique_target])

    # Inject duplicates by randomly repeating existing URLs
    pool = list(unique_urls)
    dup_count = total - len(pool)
    duplicates = rng.choices(unique_urls, k=dup_count)
    pool.extend(duplicates)

    # Shuffle so duplicates aren't all at the end
    rng.shuffle(pool)
    return pool, domain_map


def get_domain_category(url: str, domain_map: dict[str, str]) -> str:
    return domain_map.get(url, "general")
