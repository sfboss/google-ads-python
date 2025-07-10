#!/usr/bin/env python3
"""
serp2text.py – CLI to scrape Google SERP URLs, fetch pages, convert to text,
and extract associated keywords.

Requirements
------------
pip install googlesearch-python requests-html html2text yake rich configobj nest_asyncio
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Tuple

from configobj import ConfigObj
from googlesearch import search as google_search
from html2text import HTML2Text
import nest_asyncio
from requests_html import AsyncHTMLSession
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
import yake

# --------------------------------------------------------------------------- #
# Globals
# --------------------------------------------------------------------------- #
console = Console()
logger = logging.getLogger("serp2text")

DEFAULT_CFG = {
    "num_urls": 20,
    "top_n_keywords": 40,
    "lang": "en",
    "tld": "com",
    "concurrency": 5,
    "delay": 1.5,
    "timeout": 30,
    "min_characters": 500,
}

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def init_logging(log_level: str, log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handlers = [RichHandler(rich_tracebacks=True, console=console)]
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    handlers.append(file_handler)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        handlers=handlers,
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("requests_html").setLevel(logging.WARNING)


def slugify(text: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in text.lower())[:60]


def sanitize_filename(url: str) -> str:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    filename = f"{parsed.netloc}{parsed.path}".replace("/", "_")
    return filename[:200] or "index"

# --------------------------------------------------------------------------- #
# Google Search
# --------------------------------------------------------------------------- #
def get_serp_urls(keyword: str, num: int, lang: str, tld: str) -> List[str]:
    """Return *num* unique SERP URLs."""
    logger.info(f"Querying Google for '{keyword}' – expecting {num} results")
    urls = []
    for url in google_search(keyword, num=num, lang=lang, tld=tld):
        if any(bad in url for bad in ("google.com", "/settings/")):
            continue
        if url not in urls:
            urls.append(url)
        if len(urls) >= num:
            break
    logger.debug("Collected %s urls", len(urls))
    return urls

# --------------------------------------------------------------------------- #
# Async Fetch
# --------------------------------------------------------------------------- #
class URLFetcher:
    def __init__(
        self,
        urls: List[str],
        output_dir: Path,
        delay: float,
        timeout: int,
        concurrency: int,
    ):
        self.urls = urls
        self.output_dir = output_dir
        self.delay = delay
        self.timeout = timeout
        self.sem = asyncio.Semaphore(concurrency)
        self.session: AsyncHTMLSession | None = None
        self.html2text = HTML2Text()
        self.html2text.ignore_links = False
        self.html2text.ignore_images = True

    async def __aenter__(self):
        self.session = AsyncHTMLSession()
        return self

    async def __aexit__(self, *_):
        await self.session.close()

    async def fetch_one(self, url: str, progress, task_id):
        async with self.sem:
            try:
                r = await self.session.get(url, timeout=self.timeout)
                await r.html.arender(timeout=self.timeout, sleep=1, wait=1.5)
                html = r.html.html
            except Exception as e:
                logger.warning("%s failed – %s", url, e)
                progress.advance(task_id, 1)
                return None
            progress.advance(task_id, 1)
            fname = sanitize_filename(url) + ".html"
            html_path = self.output_dir / "pages" / fname
            html_path.write_text(html, encoding="utf-8", errors="ignore")
            text = self.html2text.handle(html)
            if len(text) >= DEFAULT_CFG["min_characters"]:
                html_path.with_suffix(".md").write_text(text, encoding="utf-8")
            await asyncio.sleep(self.delay)
            return text

    async def run(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "pages").mkdir(exist_ok=True)
        tasks = []
        with Progress(
            SpinnerColumn(),
            "{task.description}",
            BarColumn(),
            "{task.completed}/{task.total}",
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            task_id = progress.add_task("Fetching pages", total=len(self.urls))
            for url in self.urls:
                tasks.append(asyncio.create_task(self.fetch_one(url, progress, task_id)))
            texts = await asyncio.gather(*tasks)
        return [txt for txt in texts if txt]

# --------------------------------------------------------------------------- #
# Keyword Extraction
# --------------------------------------------------------------------------- #
def extract_keywords(
    docs: List[str], lang: str, top_n: int
) -> List[Tuple[str, float]]:
    logger.info("Extracting keywords with YAKE")
    kw_extractor = yake.KeywordExtractor(
        lan=lang, n=3, dedupLim=0.9, top=top_n, features=None
    )
    text = "\n".join(docs)
    keywords = kw_extractor.extract_keywords(text)
    keywords.sort(key=lambda x: x[1])  # lower score = better
    return keywords

# --------------------------------------------------------------------------- #
# Main helpers
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="SERP → Text → Keywords helper.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("keyword", help="Search query")
    p.add_argument("--config", type=str, help="INI config overrides")
    p.add_argument("--num-urls", type=int, default=DEFAULT_CFG["num_urls"])
    p.add_argument("--top-n", type=int, default=DEFAULT_CFG["top_n_keywords"])
    p.add_argument("--lang", default=DEFAULT_CFG["lang"])
    p.add_argument("--tld", default=DEFAULT_CFG["tld"])
    p.add_argument("--concurrency", type=int, default=DEFAULT_CFG["concurrency"])
    p.add_argument("--delay", type=float, default=DEFAULT_CFG["delay"])
    p.add_argument("--timeout", type=int, default=DEFAULT_CFG["timeout"])
    p.add_argument("--outdir", default="results", help="Base output directory")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--show", action="store_true", help="Display keyword table")
    return p.parse_args()


def load_config(path: str | None) -> Dict:
    if path and Path(path).is_file():
        cfg = ConfigObj(path)
        console.log(f"Loaded config from {path}")
        return {k: type(DEFAULT_CFG.get(k, str))(v) for k, v in cfg.items()}
    return {}


def merge_cfg(args: argparse.Namespace, cfg: Dict) -> Dict:
    merged = DEFAULT_CFG.copy()
    merged.update(cfg)
    merged.update({k: v for k, v in vars(args).items() if v is not None})
    return merged


def save_keywords(keywords, out_path: Path):
    data = [{"keyword": k, "score": s} for k, s in keywords]
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def main():
    args = parse_args()
    cfg_overrides = load_config(args.config)
    cfg = merge_cfg(args, cfg_overrides)

    keyword_slug = slugify(args.keyword)
    outdir = Path(args.outdir) / keyword_slug
    outdir.mkdir(parents=True, exist_ok=True)
    init_logging(args.log_level, outdir / "serp2text.log")

    logger.info("Running SERP2Text for '%s' → %s", args.keyword, outdir)
    urls = get_serp_urls(args.keyword, cfg["num_urls"], cfg["lang"], cfg["tld"])
    (outdir / "urls.txt").write_text("\n".join(urls), encoding="utf-8")
    logger.info("Saved urls.txt with %s entries", len(urls))

    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    async def fetch_docs():
        async with URLFetcher(
            urls,
            outdir,
            delay=cfg["delay"],
            timeout=cfg["timeout"],
            concurrency=cfg["concurrency"],
        ) as fetcher:
            return await fetcher.run()
    docs = loop.run_until_complete(fetch_docs())
    logger.info("Fetched and saved %s documents", len(docs))

    keywords = extract_keywords(docs, cfg["lang"], cfg["top_n_keywords"])
    save_keywords(keywords, outdir / "keywords.json")
    logger.info("Saved keywords.json (%s items)", len(keywords))

    if args.show:
        table = Table(
            title=f"Top keywords for '{args.keyword}'", show_lines=False
        )
        table.add_column("Rank", justify="right")
        table.add_column("Keyword", overflow="fold")
        table.add_column("Score", justify="right")
        for i, (kw, score) in enumerate(keywords, 1):
            table.add_row(str(i), kw, f"{score:.4f}")
        console.print(table)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("[bold red]Interrupted by user[/bold red]")
        sys.exit(130)
