from __future__ import annotations

import json
import hashlib
import logging
import os
import re
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder='.')
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

NEWS_SOURCES: List[Dict[str, str]] = [
    {
        'name': 'TechCrunch AI',
        'url': 'https://techcrunch.com/tag/artificial-intelligence/feed/',
        'region': 'Global',
    },
    {
        'name': 'The Verge Tech',
        'url': 'https://www.theverge.com/rss/index.xml',
        'region': 'Global',
    },
    {
        'name': 'Ars Technica',
        'url': 'https://feeds.arstechnica.com/arstechnica/index',
        'region': 'Global',
    },
    {
        'name': 'VentureBeat AI',
        'url': 'https://venturebeat.com/category/ai/feed/',
        'region': 'Global',
    },
    {
        'name': 'Wired',
        'url': 'https://www.wired.com/feed/rss',
        'region': 'Global',
    },
    {
        'name': 'NVIDIA Blog',
        'url': 'https://blogs.nvidia.com/feed/',
        'region': 'Global',
    },
    {
        'name': 'OpenAI News',
        'url': 'https://openai.com/news/rss.xml',
        'region': 'Global',
    },
    {
        'name': 'Google AI Blog',
        'url': 'https://blog.google/technology/ai/rss/',
        'region': 'Global',
    },
    {
        'name': 'MIT Technology Review',
        'url': 'https://www.technologyreview.com/feed/',
        'region': 'Global',
    },
    {
        'name': 'Semiconductor Engineering',
        'url': 'https://semiengineering.com/feed/',
        'region': 'Global',
    },
    {
        'name': 'Bloomberg Technology',
        'url': 'https://feeds.bloomberg.com/technology/news.rss',
        'region': 'Global',
    },
    {
        'name': 'Google News AI+Chips',
        'url': 'https://news.google.com/rss/search?q=artificial+intelligence+OR+semiconductor+OR+chip&hl=en-US&gl=US&ceid=US:en',
        'region': 'Global',
    },
]

X_ACCOUNTS: List[Dict[str, str]] = [
    {'label': 'OpenAI', 'handle': 'OpenAI', 'tag': 'AI Lab'},
    {'label': 'Greg Brockman', 'handle': 'gdb', 'tag': 'OpenAI Co-founder'},
    {'label': 'Anthropic', 'handle': 'Anthropic_ai', 'tag': 'AI Lab'},
    {'label': 'Claude', 'handle': 'claudeai', 'tag': 'Anthropic Product'},
    {'label': 'Google DeepMind', 'handle': 'GoogleDeepMind', 'tag': 'AI Lab'},
    {'label': 'NVIDIA', 'handle': 'nvidia', 'tag': 'Chip Leader'},
    {'label': 'Jensen Huang', 'handle': 'jensenhuang', 'tag': 'NVIDIA CEO'},
    {'label': 'Apple', 'handle': 'Apple', 'tag': 'Big Tech'},
    {'label': 'Tesla', 'handle': 'Tesla', 'tag': 'Autonomy/EV'},
    {'label': 'Elon Musk', 'handle': 'elonmusk', 'tag': 'Founder/CEO'},
    {'label': 'xAI', 'handle': 'xAI', 'tag': 'AI Lab'},
    {'label': 'Grok', 'handle': 'grok', 'tag': 'xAI Product'},
    {'label': 'OpenClaw', 'handle': 'openclaw', 'tag': 'Open Source AI'},
]
READING_WATCH_ACCOUNTS: List[Dict[str, str]] = [
    {'label': 'Elon Musk', 'handle': 'elonmusk'},
    {'label': 'Jensen Huang', 'handle': 'jensenhuang'},
]
X_EXCLUDED_HANDLES = {
    handle.strip().lower()
    for handle in os.environ.get('X_EXCLUDED_HANDLES', 'sama,darioamodei,tim_cook').split(',')
    if handle.strip()
}


def _active_x_accounts() -> List[Dict[str, str]]:
    if not X_EXCLUDED_HANDLES:
        return list(X_ACCOUNTS)
    return [
        account
        for account in X_ACCOUNTS
        if str(account.get('handle') or '').lower() not in X_EXCLUDED_HANDLES
    ]

KEYWORD_PATTERNS: Dict[str, re.Pattern[str]] = {
    'ai': re.compile(r'\b(ai|artificial intelligence)\b', re.IGNORECASE),
    'llm': re.compile(r'\b(llm|large language model|gpt|claude|gemini|grok)\b', re.IGNORECASE),
    'semiconductor': re.compile(r'\b(semiconductor|chip(?:s|set)?|foundry|fab)\b', re.IGNORECASE),
    'gpu': re.compile(r'\b(gpu|cuda|accelerator)\b', re.IGNORECASE),
    'nvidia': re.compile(r'\bnvidia\b', re.IGNORECASE),
    'amd': re.compile(r'\bamd\b', re.IGNORECASE),
    'intel': re.compile(r'\bintel\b', re.IGNORECASE),
    'tsmc': re.compile(r'\btsmc\b', re.IGNORECASE),
    'asml': re.compile(r'\basml\b', re.IGNORECASE),
    'openai': re.compile(r'\bopenai\b|\bchatgpt\b', re.IGNORECASE),
    'anthropic': re.compile(r'\banthropic\b|\bclaude(?:\s*code)?\b', re.IGNORECASE),
    'deepmind': re.compile(r'\bdeepmind\b|\bgoogle deepmind\b', re.IGNORECASE),
    'apple': re.compile(r'\bapple\b|\btim cook\b', re.IGNORECASE),
    'tesla': re.compile(r'\btesla\b|\belon musk\b', re.IGNORECASE),
    'xai': re.compile(r'\bxai\b|\bgrok\b', re.IGNORECASE),
    'openclaw': re.compile(r'\bopenclaw\b', re.IGNORECASE),
    'datacenter': re.compile(r'\b(data center|datacenter|cloud|inference)\b', re.IGNORECASE),
    'robotics': re.compile(r'\b(robotics?|autonomous|self-driving)\b', re.IGNORECASE),
    'policy': re.compile(r'\b(policy|regulation|antitrust|export controls?|governance|safety)\b', re.IGNORECASE),
}

FETCH_TIMEOUT_SECONDS = int(os.environ.get('FETCH_TIMEOUT_SECONDS', '20'))
CACHE_TTL_SECONDS = int(os.environ.get('CACHE_TTL_SECONDS', '300'))
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '14'))
MAX_ITEMS_PER_SOURCE = int(os.environ.get('MAX_ITEMS_PER_SOURCE', '60'))
X_CACHE_TTL_SECONDS = int(os.environ.get('X_CACHE_TTL_SECONDS', '180'))
X_DEFAULT_POST_LIMIT = int(os.environ.get('X_DEFAULT_POST_LIMIT', '8'))
_raw_x_token = os.environ.get('X_BEARER_TOKEN', '').strip()
# Allow users to paste either "TOKEN" or "Bearer TOKEN".
if _raw_x_token.lower().startswith('bearer '):
    _raw_x_token = _raw_x_token.split(' ', 1)[1].strip()
X_BEARER_TOKEN = _raw_x_token
X_API_BASE_URLS = ['https://api.x.com/2', 'https://api.twitter.com/2']
X_READER_FALLBACK_ENABLED = os.environ.get('X_READER_FALLBACK_ENABLED', '1').lower() in ('1', 'true', 'yes')
X_PUBLIC_METRICS_ENABLED = os.environ.get('X_PUBLIC_METRICS_ENABLED', '1').lower() in ('1', 'true', 'yes')
X_PUBLIC_CACHE_TTL_SECONDS = int(os.environ.get('X_PUBLIC_CACHE_TTL_SECONDS', '300'))
X_PUBLIC_GUEST_TOKEN_TTL_SECONDS = int(os.environ.get('X_PUBLIC_GUEST_TOKEN_TTL_SECONDS', '1800'))
X_WEB_QUERY_ID_FALLBACK = os.environ.get('X_WEB_QUERY_ID_FALLBACK', 'oSBAzPwnB3u5R9KqxACO3Q')
SUMMARY_CACHE_TTL_SECONDS = int(os.environ.get('SUMMARY_CACHE_TTL_SECONDS', '90'))
SUMMARY_DEFAULT_X_LIMIT = int(os.environ.get('SUMMARY_DEFAULT_X_LIMIT', '3'))
SUMMARY_TRANSLATION_ENABLED = os.environ.get('SUMMARY_TRANSLATION_ENABLED', '1').lower() in ('1', 'true', 'yes')
READING_WATCH_DEFAULT_LIMIT = int(os.environ.get('READING_WATCH_DEFAULT_LIMIT', '6'))
READING_WATCH_MIN_STATUS_ID = int(os.environ.get('READING_WATCH_MIN_STATUS_ID', '1600000000000000000'))

DATE_FORMATS = [
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%SZ',
    '%Y-%m-%d %H:%M:%S%z',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d',
]

session = requests.Session()
session.headers.update(
    {
        'User-Agent': 'AITechNewsHub/1.0 (+https://localhost)',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    }
)

x_session = requests.Session()
x_session.headers.update(
    {
        'User-Agent': 'AITechNewsHub/1.0 (+https://localhost)',
        'Accept': 'application/json',
    }
)
if X_BEARER_TOKEN:
    x_session.headers['Authorization'] = f'Bearer {X_BEARER_TOKEN}'

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {
    'updated_at': 0.0,
    'news': [],
    'source_errors': [],
}

_x_cache_lock = threading.Lock()
_x_user_id_cache: Dict[str, str] = {}
_x_posts_cache: Dict[str, Dict[str, Any]] = {}

_x_public_lock = threading.Lock()
_x_public_metrics_cache: Dict[str, Dict[str, Any]] = {}
_x_public_web_cache: Dict[str, Any] = {'updated_at': 0.0, 'bearer': '', 'query_id': X_WEB_QUERY_ID_FALLBACK}
_x_public_guest_cache: Dict[str, Any] = {'updated_at': 0.0, 'guest_token': ''}

_summary_lock = threading.Lock()
_summary_cache: Dict[str, Dict[str, Any]] = {}
_translation_lock = threading.Lock()
_translation_cache: Dict[str, str] = {}

SENTENCE_SPLIT_PATTERN = re.compile(r'(?<=[\.\!\?])\s+')
ACTION_ESCALATION_PATTERN = re.compile(
    r'\b(launch(?:ed|es|ing)?|release(?:d|s)?|unveil(?:ed|s)?|announce(?:d|s|ment)?|ship(?:ped|s|ping)?|'
    r'roll(?:ed)?\s*out|deploy(?:ed|s|ment)?|partner(?:s|ed|ship)?|acquir(?:e|ed|es)|'
    r'invest(?:ed|ment|s)?|funding|raise(?:d|s)?|expan(?:d|ded|s)|build(?:ing|s)?|production|roadmap)\b',
    re.IGNORECASE,
)
ACTION_DIPLOMACY_PATTERN = re.compile(
    r'\b(policy|regulation|regulatory|governance|standard(?:s)?|compliance|export controls?|'
    r'antitrust|hearing|bill|law|approval|licensing)\b',
    re.IGNORECASE,
)
ACTION_DEFENSIVE_PATTERN = re.compile(
    r'\b(security|privacy|alignment|safety|guardrails?|risk mitigation|benchmark|performance|'
    r'efficiency|latency|throughput|inference|training)\b',
    re.IGNORECASE,
)
ACTION_WARNING_PATTERN = re.compile(
    r'\b(warn(?:ing|ed|s)?|risk(?:s)?|concern(?:s)?|shortage(?:s)?|delay(?:ed|s)?|'
    r'ban(?:ned|s)?|probe(?:d|s)?|lawsuit(?:s)?|vulnerabilit(?:y|ies)|outage(?:s)?)\b',
    re.IGNORECASE,
)
FUNDING_MARKET_PATTERN = re.compile(
    r'\b(funding|raise(?:d|s)?|valuation|market cap|stock|shares?|earnings?|revenue|guidance|'
    r'ipo|acquisition|merger|deal|investment|investor|backed|capex|orders?|bookings?)\b',
    re.IGNORECASE,
)

PARTY_PATTERNS: Dict[str, re.Pattern[str]] = {
    'OpenAI': re.compile(
        r'\b(openai|chatgpt|gpt-?\d|sam altman)\b',
        re.IGNORECASE,
    ),
    'Anthropic/Claude': re.compile(r'\b(anthropic|claude(?:\s*code)?|dario amodei)\b', re.IGNORECASE),
    'Google/DeepMind': re.compile(r'\b(google|deepmind|gemini|alphafold)\b', re.IGNORECASE),
    'Microsoft': re.compile(r'\b(microsoft|azure|copilot)\b', re.IGNORECASE),
    'NVIDIA': re.compile(r'\b(nvidia|cuda|h100|blackwell)\b', re.IGNORECASE),
    'Apple': re.compile(r'\b(apple|tim cook)\b', re.IGNORECASE),
    'Tesla/xAI': re.compile(r'\b(tesla|elon musk|xai|grok)\b', re.IGNORECASE),
    'Chip Makers': re.compile(r'\b(amd|intel|tsmc|asml|qualcomm|broadcom|samsung)\b', re.IGNORECASE),
}

CITY_PATTERNS: Dict[str, re.Pattern[str]] = {
    'Foundation Models': re.compile(r'\b(llm|foundation model|gpt|claude|gemini|model weights)\b', re.IGNORECASE),
    'Chips & Foundry': re.compile(r'\b(semiconductor|chip|gpu|cpu|foundry|fab|wafer|asml|tsmc)\b', re.IGNORECASE),
    'Data Centers & Cloud': re.compile(r'\b(data center|datacenter|cloud|inference cluster|hpc)\b', re.IGNORECASE),
    'Robotics & Autonomy': re.compile(r'\b(robotics?|autonomous|self-driving|humanoid)\b', re.IGNORECASE),
    'Policy & Regulation': re.compile(r'\b(policy|regulation|export controls?|antitrust|governance)\b', re.IGNORECASE),
    'Open Source AI': re.compile(r'\b(open source|open-weights|weights release)\b', re.IGNORECASE),
}

CASUALTY_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    (
        'money',
        re.compile(
            r'\$(?P<count>\d[\d,]*(?:\.\d+)?)\s*(?P<unit>billion|million|bn|m)?\b',
            re.IGNORECASE,
        ),
    ),
    (
        'percent',
        re.compile(r'\b(?P<count>\d{1,3})\s*%', re.IGNORECASE),
    ),
    (
        'process-node',
        re.compile(r'\b(?P<count>\d{1,2})\s*nm\b', re.IGNORECASE),
    ),
    (
        'volume',
        re.compile(
            r'\b(?P<count>\d[\d,]{0,6})\s*(?:gpu|gpus|chips|servers|nodes|customers|users)\b',
            re.IGNORECASE,
        ),
    ),
]


def strip_html(value: Optional[str]) -> str:
    if not value:
        return ''
    no_tags = re.sub(r'<[^>]+>', ' ', value)
    return re.sub(r'\s+', ' ', unescape(no_tags)).strip()


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None

    raw = value.strip()

    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except (TypeError, ValueError):
        pass

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def node_text(node: Optional[ET.Element]) -> str:
    if node is None:
        return ''
    text = ''.join(node.itertext())
    return text.strip()


def find_text(element: ET.Element, paths: List[str]) -> str:
    for path in paths:
        node = element.find(path)
        if node is not None:
            text = node_text(node)
            if text:
                return text
    return ''


def find_link(element: ET.Element) -> str:
    link_node = element.find('link')
    if link_node is not None:
        link_text = node_text(link_node)
        if link_text:
            return link_text

    for link_candidate in element.findall('{*}link'):
        href = (link_candidate.attrib.get('href') or '').strip()
        rel = (link_candidate.attrib.get('rel') or 'alternate').strip()
        if href and rel in ('', 'alternate'):
            return href

    guid = find_text(element, ['guid', '{*}guid'])
    if guid.startswith('http://') or guid.startswith('https://'):
        return guid

    return ''


def match_keywords(text: str) -> List[str]:
    if not text:
        return []

    matches: List[str] = []
    for label, pattern in KEYWORD_PATTERNS.items():
        if pattern.search(text):
            matches.append(label)
    return matches


def parse_rss_item(item: ET.Element, source: Dict[str, str], cutoff: datetime) -> Optional[Dict[str, Any]]:
    title = find_text(item, ['title', '{*}title'])
    link = find_link(item)
    description = find_text(
        item,
        [
            'description',
            '{*}description',
            '{http://purl.org/rss/1.0/modules/content/}encoded',
            '{*}content',
            '{*}summary',
        ],
    )
    published_raw = find_text(
        item,
        [
            'pubDate',
            '{*}pubDate',
            '{http://purl.org/dc/elements/1.1/}date',
            '{*}date',
            '{*}published',
            '{*}updated',
        ],
    )

    published_at = parse_datetime(published_raw) or datetime.now(timezone.utc)
    if published_at < cutoff:
        return None

    clean_description = strip_html(description)
    clean_title = strip_html(title)
    if not clean_title:
        return None

    joined = f'{clean_title} {clean_description}'
    matched = match_keywords(joined)
    if not matched:
        return None

    item_id = hashlib.sha1(f"{source['name']}|{link}|{clean_title}".encode('utf-8')).hexdigest()[:16]

    return {
        'id': item_id,
        'title': clean_title,
        'url': link,
        'summary': clean_description[:320],
        'published_at': published_at.isoformat(),
        'published_ts': int(published_at.timestamp() * 1000),
        'source': source['name'],
        'region': source['region'],
        'matched_keywords': matched,
    }


def parse_feed(xml_content: bytes, source: Dict[str, str]) -> List[Dict[str, Any]]:
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as exc:
        raise ValueError(f"invalid XML from {source['name']}: {exc}") from exc

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    rss_items = root.findall('.//item')
    atom_entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')

    if not atom_entries:
        atom_entries = root.findall('.//entry')

    nodes = rss_items if rss_items else atom_entries

    parsed: List[Dict[str, Any]] = []
    for node in nodes[:MAX_ITEMS_PER_SOURCE]:
        article = parse_rss_item(node, source, cutoff)
        if article:
            parsed.append(article)

    return parsed


def fetch_source(source: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        logger.info('Fetching %s', source['name'])
        response = session.get(source['url'], timeout=FETCH_TIMEOUT_SECONDS)
        response.raise_for_status()
        items = parse_feed(response.content, source)
        logger.info('Fetched %s: %d matched items', source['name'], len(items))
        return items, None
    except Exception as exc:
        error = f"{source['name']}: {exc}"
        logger.warning(error)
        return [], error


def dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for item in sorted(items, key=lambda value: value['published_ts'], reverse=True):
        key = item['url'].strip() if item['url'] else f"{item['source']}::{item['title'].lower()}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def _x_error_message(response: requests.Response) -> str:
    if response.status_code == 401:
        return 'X API authentication failed (401). Check X_BEARER_TOKEN.'
    if response.status_code == 403:
        return 'X API access forbidden (403). Your token/app may lack permissions.'
    if response.status_code == 429:
        return 'X API rate limit reached (429). Try again later.'
    try:
        payload = response.json()
        detail = payload.get('detail') or payload.get('title') or ''
        if detail:
            return f'X API {response.status_code}: {detail}'
    except ValueError:
        pass
    return f'X API error {response.status_code}'


def call_x_api(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not X_BEARER_TOKEN:
        raise RuntimeError('X API is not configured. Set X_BEARER_TOKEN on the server.')

    last_error = 'X API request failed'

    for base_url in X_API_BASE_URLS:
        url = f'{base_url}{path}'
        try:
            response = x_session.get(url, params=params, timeout=FETCH_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            last_error = f'{base_url}: {exc}'
            continue

        if response.status_code >= 400:
            error = _x_error_message(response)
            if response.status_code == 404:
                last_error = error
                continue
            raise RuntimeError(error)

        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(f'X API returned invalid JSON from {base_url}') from exc

    raise RuntimeError(last_error)


def resolve_x_user_id(handle: str) -> str:
    normalized = handle.strip().lstrip('@').lower()
    with _x_cache_lock:
        cached = _x_user_id_cache.get(normalized)
        if cached:
            return cached

    payload = call_x_api(
        f'/users/by/username/{normalized}',
        {'user.fields': 'id,name,username,profile_image_url'},
    )
    data = payload.get('data') or {}
    user_id = str(data.get('id') or '').strip()
    if not user_id:
        raise RuntimeError(f'Unable to resolve X user ID for @{normalized}')

    with _x_cache_lock:
        _x_user_id_cache[normalized] = user_id

    return user_id


def expand_x_urls(text: str, entities: Dict[str, Any]) -> str:
    if not text:
        return ''

    expanded = text
    for url_item in entities.get('urls', []) or []:
        short = str(url_item.get('url') or '').strip()
        target = str(url_item.get('expanded_url') or url_item.get('display_url') or '').strip()
        if short and target:
            expanded = expanded.replace(short, target)
    return expanded.strip()


def _extract_status_id_from_url(url: str) -> str:
    match = re.search(r'https://x\.com/[A-Za-z0-9_]+/status/(\d+)', url or '')
    return match.group(1) if match else ''


def _resolve_x_web_auth(force: bool = False) -> Tuple[str, str]:
    now = time.time()
    with _x_public_lock:
        age = now - float(_x_public_web_cache.get('updated_at', 0.0))
        bearer_cached = str(_x_public_web_cache.get('bearer', '')).strip()
        query_cached = str(_x_public_web_cache.get('query_id', '')).strip()
        if not force and bearer_cached and query_cached and age < X_PUBLIC_GUEST_TOKEN_TTL_SECONDS:
            return bearer_cached, query_cached

    page_html = session.get('https://x.com/CNN', timeout=FETCH_TIMEOUT_SECONDS).text
    main_js_match = re.search(
        r'https://abs\.twimg\.com/responsive-web/client-web/main\.[^\" ]+\.js',
        page_html,
    )
    if not main_js_match:
        raise RuntimeError('Could not locate X main JS bundle for public metrics.')

    main_js_url = main_js_match.group(0)
    js_text = session.get(main_js_url, timeout=FETCH_TIMEOUT_SECONDS).text

    bearer_match = re.search(r'AAAAAAAAAAAAAAAAAAAAA[^\"\s]{20,250}', js_text)
    if not bearer_match:
        raise RuntimeError('Could not extract public X web bearer token.')
    bearer = urllib.parse.unquote(bearer_match.group(0))

    query_match = re.search(
        r'queryId:\"([A-Za-z0-9_-]+)\",operationName:\"TweetResultByRestId\"',
        js_text,
    )
    query_id = query_match.group(1) if query_match else X_WEB_QUERY_ID_FALLBACK

    with _x_public_lock:
        _x_public_web_cache['updated_at'] = time.time()
        _x_public_web_cache['bearer'] = bearer
        _x_public_web_cache['query_id'] = query_id

    return bearer, query_id


def _get_public_guest_token(bearer: str, force: bool = False) -> str:
    now = time.time()
    with _x_public_lock:
        age = now - float(_x_public_guest_cache.get('updated_at', 0.0))
        cached = str(_x_public_guest_cache.get('guest_token', '')).strip()
        if not force and cached and age < X_PUBLIC_GUEST_TOKEN_TTL_SECONDS:
            return cached

    response = session.post(
        'https://api.x.com/1.1/guest/activate.json',
        headers={
            'authorization': f'Bearer {bearer}',
            'content-type': 'application/json',
        },
        timeout=FETCH_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    guest_token = str(payload.get('guest_token') or '').strip()
    if not guest_token:
        raise RuntimeError('Guest token not returned by X public auth.')

    with _x_public_lock:
        _x_public_guest_cache['updated_at'] = time.time()
        _x_public_guest_cache['guest_token'] = guest_token

    return guest_token


def _fetch_public_tweet_result(status_id: str) -> Optional[Dict[str, Any]]:
    if not status_id:
        return None

    now = time.time()
    with _x_public_lock:
        cached = _x_public_metrics_cache.get(status_id)
        if cached and (now - float(cached.get('updated_at', 0.0))) < X_PUBLIC_CACHE_TTL_SECONDS:
            return dict(cached.get('result') or {})

    try:
        bearer, query_id = _resolve_x_web_auth()
        guest_token = _get_public_guest_token(bearer)

        variables = {
            'tweetId': status_id,
            'withCommunity': False,
            'includePromotedContent': False,
            'withVoice': True,
        }
        features = {
            'responsive_web_graphql_exclude_directive_enabled': True,
            'responsive_web_graphql_skip_user_profile_image_extensions_enabled': False,
            'responsive_web_graphql_timeline_navigation_enabled': True,
            'tweetypie_unmention_optimization_enabled': True,
            'vibe_api_enabled': True,
            'responsive_web_edit_tweet_api_enabled': True,
            'graphql_is_translatable_rweb_tweet_is_translatable_enabled': True,
            'view_counts_everywhere_api_enabled': True,
            'longform_notetweets_consumption_enabled': True,
            'responsive_web_twitter_article_tweet_consumption_enabled': True,
            'tweet_awards_web_tipping_enabled': False,
            'creator_subscriptions_quote_tweet_preview_enabled': False,
            'freedom_of_speech_not_reach_fetch_enabled': True,
            'standardized_nudges_misinfo': True,
            'tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled': True,
            'rweb_video_timestamps_enabled': True,
            'longform_notetweets_rich_text_read_enabled': True,
            'longform_notetweets_inline_media_enabled': True,
            'responsive_web_enhance_cards_enabled': False,
        }

        params = {
            'variables': json_compact(variables),
            'features': json_compact(features),
        }

        headers = {
            'authorization': f'Bearer {bearer}',
            'x-guest-token': guest_token,
            'x-twitter-active-user': 'yes',
            'x-twitter-client-language': 'en',
            'accept': 'application/json',
        }

        url = f'https://x.com/i/api/graphql/{query_id}/TweetResultByRestId'
        response = session.get(url, params=params, headers=headers, timeout=FETCH_TIMEOUT_SECONDS)
        if response.status_code in (401, 403):
            guest_token = _get_public_guest_token(bearer, force=True)
            headers['x-guest-token'] = guest_token
            response = session.get(url, params=params, headers=headers, timeout=FETCH_TIMEOUT_SECONDS)

        if response.status_code >= 400:
            return None

        payload = response.json()
        result = (
            payload.get('data', {})
            .get('tweetResult', {})
            .get('result', {})
        )
        if not isinstance(result, dict):
            return None
        if result.get('__typename') not in ('Tweet', 'TweetWithVisibilityResults'):
            return None

        legacy = result.get('legacy') or {}
        if result.get('__typename') == 'TweetWithVisibilityResults':
            inner = result.get('tweet') or {}
            if isinstance(inner, dict):
                legacy = inner.get('legacy') or legacy

        media_urls: List[str] = []
        for media in (legacy.get('extended_entities') or {}).get('media', []) or []:
            media_url = str(media.get('media_url_https') or media.get('media_url') or '').strip()
            if media_url and media_url not in media_urls:
                media_urls.append(media_url)
        if not media_urls:
            for media in (legacy.get('entities') or {}).get('media', []) or []:
                media_url = str(media.get('media_url_https') or media.get('media_url') or '').strip()
                if media_url and media_url not in media_urls:
                    media_urls.append(media_url)

        parsed = {
            'reply_count': int(legacy.get('reply_count', 0)),
            'retweet_count': int(legacy.get('retweet_count', 0)),
            'like_count': int(legacy.get('favorite_count', 0)),
            'quote_count': int(legacy.get('quote_count', 0)),
            'media_urls': media_urls,
            'media_url': media_urls[0] if media_urls else '',
        }

        with _x_public_lock:
            _x_public_metrics_cache[status_id] = {'updated_at': time.time(), 'result': dict(parsed)}

        return parsed
    except Exception:
        return None


def _enrich_posts_with_public_metrics(posts: List[Dict[str, Any]]) -> int:
    if not X_PUBLIC_METRICS_ENABLED:
        return 0

    enriched = 0
    for post in posts:
        status_id = _extract_status_id_from_url(str(post.get('url') or ''))
        if not status_id:
            continue
        metrics = _fetch_public_tweet_result(status_id)
        if not metrics:
            continue

        for key in ('reply_count', 'retweet_count', 'like_count', 'quote_count'):
            if key in metrics:
                post[key] = metrics[key]

        media_urls = metrics.get('media_urls') or []
        if media_urls and not post.get('media_urls'):
            post['media_urls'] = list(media_urls)
        if metrics.get('media_url') and not post.get('media_url'):
            post['media_url'] = metrics['media_url']
        enriched += 1

    return enriched


def json_compact(value: Dict[str, Any]) -> str:
    return json.dumps(value, separators=(',', ':'))


def _to_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_iso_to_ts(value: Any) -> float:
    dt = parse_datetime(str(value or ''))
    return dt.timestamp() if dt else 0.0


def _truncate(value: str, limit: int = 160) -> str:
    text = re.sub(r'\s+', ' ', str(value or '')).strip()
    if len(text) <= limit:
        return text
    return f'{text[: max(0, limit - 1)].rstrip()}…'


def _post_engagement_score(post: Dict[str, Any]) -> int:
    score = 0
    for key in ('like_count', 'retweet_count', 'reply_count', 'quote_count'):
        value = _to_int_or_none(post.get(key))
        if value is not None and value > 0:
            score += value
    return score


def _split_sentences(value: str) -> List[str]:
    cleaned = re.sub(r'\s+', ' ', strip_html(value or '')).strip(' \t\r\n-•|')
    if not cleaned:
        return []
    pieces = SENTENCE_SPLIT_PATTERN.split(cleaned)
    sentences = [piece.strip(' \t\r\n-•|') for piece in pieces if len(piece.strip()) >= 35]
    if sentences:
        return sentences
    return [cleaned] if len(cleaned) >= 35 else []


def _detect_stance_tags(sentence: str) -> List[str]:
    tags: List[str] = []
    if ACTION_ESCALATION_PATTERN.search(sentence):
        tags.append('growth')
    if ACTION_DEFENSIVE_PATTERN.search(sentence):
        tags.append('technical')
    if ACTION_DIPLOMACY_PATTERN.search(sentence):
        tags.append('policy')
    if ACTION_WARNING_PATTERN.search(sentence):
        tags.append('warning')
    if FUNDING_MARKET_PATTERN.search(sentence):
        tags.append('market')
    return tags


def _stance_text(tag: str) -> str:
    if tag == 'growth':
        return 'highlights launches, partnerships, or expansion moves'
    if tag == 'technical':
        return 'focuses on model/chip performance and engineering progress'
    if tag == 'policy':
        return 'emphasizes policy, regulation, or compliance'
    if tag == 'market':
        return 'emphasizes funding, valuation, earnings, or market signals'
    if tag == 'warning':
        return 'signals risks, delays, or supply constraints'
    return 'shows mixed or unclear positioning in current reporting'


def _extract_casualty_mentions(sentence: str) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    seen: set[Tuple[str, int]] = set()
    max_value_by_kind = {
        'money': 20_000_000_000_000,  # up to tens of trillions
        'percent': 500,
        'process-node': 50,
        'volume': 500_000_000,
    }
    for kind, pattern in CASUALTY_PATTERNS:
        for match in pattern.finditer(sentence):
            raw_count = str(match.groupdict().get('count') or '').replace(',', '').strip()
            if not raw_count:
                continue
            try:
                if '.' in raw_count:
                    base_value = float(raw_count)
                else:
                    base_value = float(int(raw_count))
            except ValueError:
                continue

            if kind == 'money':
                unit = str(match.groupdict().get('unit') or '').lower().strip()
                if unit in ('billion', 'bn'):
                    base_value *= 1_000_000_000
                elif unit in ('million', 'm'):
                    base_value *= 1_000_000

            count = int(round(base_value))
            # Defensive cap against accidental date-like extraction.
            max_value = max_value_by_kind.get(kind, 200_000)
            if count <= 0 or count > max_value:
                continue
            key = (kind, count)
            if key in seen:
                continue
            seen.add(key)
            found.append({'kind': kind, 'count': count})
    return found


def _normalize_translation_text(text: str) -> str:
    return re.sub(r'\s+', ' ', str(text or '')).strip()


def _translate_single_normalized_to_zh(normalized: str) -> str:
    if not normalized or not SUMMARY_TRANSLATION_ENABLED:
        return ''

    translated = ''
    try:
        response = session.get(
            'https://translate.googleapis.com/translate_a/single',
            params={
                'client': 'gtx',
                'sl': 'en',
                'tl': 'zh-CN',
                'dt': 't',
                'q': normalized,
            },
            timeout=min(FETCH_TIMEOUT_SECONDS, 15),
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload and isinstance(payload[0], list):
            translated = ''.join(
                str(part[0])
                for part in payload[0]
                if isinstance(part, list) and part and part[0]
            )
        translated = re.sub(r'\s+', ' ', translated).strip()
    except Exception:
        translated = ''
    return translated


def _chunk_translation_texts(texts: List[str], max_items: int = 14, max_chars: int = 2600) -> List[List[str]]:
    chunks: List[List[str]] = []
    current: List[str] = []
    current_chars = 0
    for text in texts:
        text_chars = len(text)
        projected = current_chars + text_chars + (1 if current else 0)
        if current and (len(current) >= max_items or projected > max_chars):
            chunks.append(current)
            current = [text]
            current_chars = text_chars
            continue
        current.append(text)
        current_chars = projected
    if current:
        chunks.append(current)
    return chunks


def _translate_chunk_to_zh(chunk: List[str]) -> List[str]:
    if not chunk:
        return []
    try:
        response = session.get(
            'https://translate.googleapis.com/translate_a/single',
            params={
                'client': 'gtx',
                'sl': 'en',
                'tl': 'zh-CN',
                'dt': 't',
                'q': '\n'.join(chunk),
            },
            timeout=min(FETCH_TIMEOUT_SECONDS, 20),
        )
        response.raise_for_status()
        payload = response.json()
        if not (isinstance(payload, list) and payload and isinstance(payload[0], list)):
            return []
        translated_full = ''.join(
            str(part[0])
            for part in payload[0]
            if isinstance(part, list) and part and part[0] is not None
        )
        lines = translated_full.rstrip('\n').split('\n') if translated_full else []
        lines = [line.strip() for line in lines]
        if len(lines) != len(chunk):
            return []
        return lines
    except Exception:
        return []


def _translate_batch_to_zh(texts: List[str]) -> Dict[str, str]:
    normalized_unique: List[str] = []
    seen: set[str] = set()
    for text in texts:
        normalized = _normalize_translation_text(text)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_unique.append(normalized)

    if not normalized_unique:
        return {}

    results: Dict[str, str] = {}
    with _translation_lock:
        for normalized in normalized_unique:
            cached = _translation_cache.get(normalized)
            if cached is not None:
                results[normalized] = cached

    missing = [normalized for normalized in normalized_unique if normalized not in results]

    updates: Dict[str, str] = {}
    if missing and SUMMARY_TRANSLATION_ENABLED:
        for chunk in _chunk_translation_texts(missing):
            translated_chunk = _translate_chunk_to_zh(chunk)
            if len(translated_chunk) == len(chunk):
                for source_text, translated_text in zip(chunk, translated_chunk):
                    updates[source_text] = translated_text
            else:
                for source_text in chunk:
                    updates[source_text] = _translate_single_normalized_to_zh(source_text)
    elif missing:
        for source_text in missing:
            updates[source_text] = ''

    if updates:
        with _translation_lock:
            _translation_cache.update(updates)
        results.update(updates)

    for normalized in normalized_unique:
        results.setdefault(normalized, '')

    return results


def _translate_to_zh(text: str) -> str:
    normalized = _normalize_translation_text(text)
    if not normalized:
        return ''
    return _translate_batch_to_zh([normalized]).get(normalized, '')


def _add_news_translations(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not articles:
        return []

    texts: List[str] = []
    for row in articles:
        texts.append(str(row.get('title') or ''))
        texts.append(str(row.get('summary') or ''))
    translated = _translate_batch_to_zh(texts)

    translated_rows: List[Dict[str, Any]] = []
    for row in articles:
        title = str(row.get('title') or '')
        summary = str(row.get('summary') or '')
        out = dict(row)
        out['title_zh'] = translated.get(_normalize_translation_text(title), '')
        out['summary_zh'] = translated.get(_normalize_translation_text(summary), '')
        translated_rows.append(out)
    return translated_rows


def _add_x_post_translations(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not posts:
        return []

    texts = [str(row.get('text') or '') for row in posts]
    translated = _translate_batch_to_zh(texts)

    translated_rows: List[Dict[str, Any]] = []
    for row in posts:
        text = str(row.get('text') or '')
        out = dict(row)
        out['text_zh'] = translated.get(_normalize_translation_text(text), '')
        translated_rows.append(out)
    return translated_rows


def _build_summary_zh(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not SUMMARY_TRANSLATION_ENABLED:
        return {}

    funding_market_signals = payload.get('funding_market_signals') or payload.get('casualty_reports') or {}
    largest_funding_mention = funding_market_signals.get('largest_funding_mention_usd')
    if largest_funding_mention is None:
        largest_funding_mention = funding_market_signals.get('largest_money_mention')
    largest_market_percent = funding_market_signals.get('largest_market_percent_mention')
    if largest_market_percent is None:
        largest_market_percent = funding_market_signals.get('largest_percent_mention')

    summary_zh = {
        'summary': _translate_to_zh(str(payload.get('summary') or '')),
        'overview': _translate_to_zh(str(payload.get('overview') or payload.get('summary') or '')),
        'bullets': [_translate_to_zh(str(line or '')) for line in (payload.get('bullets') or [])],
        'latest_news_updates': [],
        'company_updates': [],
        'tech_developments': [],
        'funding_market_updates': [],
        'funding_market_signals': {
            'largest_funding_mention_usd': largest_funding_mention,
            'largest_market_percent_mention': largest_market_percent,
            'smallest_process_node_nm': funding_market_signals.get('smallest_process_node_nm'),
            'note': _translate_to_zh(str(funding_market_signals.get('note') or '')),
        },
        'conflict_trajectory': [],
        'party_attitudes': [],
        'impacted_cities': [],
        'casualty_reports': {
            'largest_money_mention': largest_funding_mention,
            'largest_percent_mention': largest_market_percent,
            'smallest_process_node_nm': funding_market_signals.get('smallest_process_node_nm'),
            'note': _translate_to_zh(str(funding_market_signals.get('note') or '')),
            'recent_reports': [],
        },
    }

    latest_updates = payload.get('latest_news_updates') or payload.get('conflict_trajectory') or []
    for row in latest_updates:
        translated = {
            'text': _translate_to_zh(str(row.get('text') or '')),
        }
        summary_zh['latest_news_updates'].append(translated)
        summary_zh['conflict_trajectory'].append(
            {
                'text': translated['text'],
            }
        )

    company_updates = payload.get('company_updates') or payload.get('party_attitudes') or []
    for row in company_updates:
        company_name = str(row.get('company') or row.get('party') or '')
        update_text = str(row.get('update') or row.get('attitude') or '')
        evidence_text = str(row.get('evidence') or '')
        translated = {
            'company': _translate_to_zh(company_name),
            'update': _translate_to_zh(update_text),
            'evidence': _translate_to_zh(evidence_text),
        }
        summary_zh['company_updates'].append(translated)
        summary_zh['party_attitudes'].append(
            {
                'party': translated['company'],
                'attitude': translated['update'],
                'evidence': translated['evidence'],
            }
        )

    tech_developments = payload.get('tech_developments') or payload.get('impacted_cities') or []
    for row in tech_developments:
        theme_text = str(row.get('theme') or row.get('city') or '')
        detail_text = str(row.get('detail') or row.get('last_mention') or '')
        translated = {
            'theme': _translate_to_zh(theme_text),
            'detail': _translate_to_zh(detail_text),
        }
        summary_zh['tech_developments'].append(translated)
        summary_zh['impacted_cities'].append(
            {
                'city': translated['theme'],
                'last_mention': translated['detail'],
            }
        )

    funding_rows = payload.get('funding_market_updates') or (payload.get('casualty_reports') or {}).get('recent_reports') or []
    for row in funding_rows:
        kind = str(row.get('kind') or '')
        kind_zh = {
            'money': '金额',
            'percent': '百分比',
            'process-node': '制程节点',
            'volume': '数量',
            'market': '市场',
        }.get(kind, '')
        translated = {
            'kind': kind_zh,
            'text': _translate_to_zh(str(row.get('text') or '')),
        }
        summary_zh['funding_market_updates'].append(translated)
        summary_zh['casualty_reports']['recent_reports'].append(
            {
                'kind': translated['kind'],
                'text': translated['text'],
            }
        )

    return summary_zh


def _build_realtime_summary(force: bool = False, x_limit: int = SUMMARY_DEFAULT_X_LIMIT) -> Dict[str, Any]:
    bounded_x_limit = max(1, min(int(x_limit), 10))
    cache_key = f'x_limit:{bounded_x_limit}'
    now = time.time()

    with _summary_lock:
        cached = _summary_cache.get(cache_key)
        if cached and not force and (now - float(cached.get('updated_at', 0.0))) < SUMMARY_CACHE_TTL_SECONDS:
            return dict(cached.get('payload') or {})

    news_payload = refresh_cache(force=force)
    articles = list(news_payload.get('news') or [])
    source_errors = list(news_payload.get('source_errors') or [])

    source_counter: Counter[str] = Counter()
    keyword_counter: Counter[str] = Counter()
    for article in articles:
        source = str(article.get('source') or 'Unknown').strip()
        source_counter[source] += 1
        for keyword in article.get('matched_keywords') or []:
            keyword_counter[str(keyword)] += 1

    x_posts: List[Dict[str, Any]] = []
    x_mode_counter: Counter[str] = Counter()
    x_errors: List[str] = []
    x_source_count: Counter[str] = Counter()
    active_x_accounts = _active_x_accounts()

    if active_x_accounts:
        with ThreadPoolExecutor(max_workers=min(6, len(active_x_accounts))) as pool:
            future_map = {
                pool.submit(
                    fetch_x_posts_for_handle,
                    account['handle'],
                    bounded_x_limit,
                    force,
                    not bool(account.get('disable_reader_fallback', False)),
                ): account
                for account in active_x_accounts
            }
            for future in as_completed(future_map):
                account = future_map[future]
                handle = account['handle']
                label = account.get('label', handle)
                try:
                    data = future.result()
                except Exception as exc:
                    x_errors.append(f'@{handle}: {exc}')
                    continue

                mode = str(data.get('mode') or 'unknown')
                x_mode_counter[mode] += 1

                error = str(data.get('error') or '').strip()
                if error:
                    x_errors.append(f'@{handle}: {error}')

                posts = data.get('posts') or []
                if posts:
                    x_source_count[handle] += len(posts)

                for row in posts:
                    text = str(row.get('text') or '').strip()
                    for keyword in match_keywords(text):
                        keyword_counter[keyword] += 1

                    x_posts.append(
                        {
                            'handle': handle,
                            'label': label,
                            'created_at': str(row.get('created_at') or ''),
                            'url': str(row.get('url') or f'https://x.com/{handle}'),
                            'text': text,
                            'like_count': _to_int_or_none(row.get('like_count')),
                            'retweet_count': _to_int_or_none(row.get('retweet_count')),
                            'reply_count': _to_int_or_none(row.get('reply_count')),
                            'quote_count': _to_int_or_none(row.get('quote_count')),
                        }
                    )

    # Build sentence-level evidence pool from both news and X content.
    evidence_rows: List[Dict[str, Any]] = []
    for article in articles:
        title = str(article.get('title') or '').strip()
        summary = str(article.get('summary') or '').strip()
        combined = '. '.join([part for part in [title, summary] if part])
        if not combined:
            continue
        for sentence in _split_sentences(combined):
            evidence_rows.append(
                {
                    'text': sentence,
                    'created_at': str(article.get('published_at') or ''),
                    'ts': _parse_iso_to_ts(article.get('published_at')),
                    'source': str(article.get('source') or 'News'),
                    'url': str(article.get('url') or ''),
                    'kind': 'news',
                }
            )

    for post in x_posts:
        for sentence in _split_sentences(str(post.get('text') or '')):
            evidence_rows.append(
                {
                    'text': sentence,
                    'created_at': str(post.get('created_at') or ''),
                    'ts': _parse_iso_to_ts(post.get('created_at')),
                    'source': f"@{post.get('handle')}",
                    'url': str(post.get('url') or ''),
                    'kind': 'x',
                }
            )

    evidence_rows.sort(key=lambda row: row.get('ts', 0.0), reverse=True)

    latest_news_updates: List[Dict[str, Any]] = []
    seen_latest_updates: set[str] = set()
    for row in evidence_rows:
        text = row['text']
        is_relevant = (
            ACTION_ESCALATION_PATTERN.search(text)
            or ACTION_DEFENSIVE_PATTERN.search(text)
            or ACTION_DIPLOMACY_PATTERN.search(text)
            or ACTION_WARNING_PATTERN.search(text)
            or FUNDING_MARKET_PATTERN.search(text)
        )
        if not is_relevant:
            for pattern in CITY_PATTERNS.values():
                if pattern.search(text):
                    is_relevant = True
                    break
        if not is_relevant:
            continue
        key = text.lower()
        if key in seen_latest_updates:
            continue
        seen_latest_updates.add(key)
        latest_news_updates.append(
            {
                'text': _truncate(text, 190),
                'source': row['source'],
                'url': row['url'],
                'created_at': row['created_at'],
                'kind': row['kind'],
            }
        )
        if len(latest_news_updates) >= 8:
            break

    if not latest_news_updates:
        for row in evidence_rows[:8]:
            text = str(row.get('text') or '').strip()
            if not text:
                continue
            key = text.lower()
            if key in seen_latest_updates:
                continue
            seen_latest_updates.add(key)
            latest_news_updates.append(
                {
                    'text': _truncate(text, 190),
                    'source': str(row.get('source') or ''),
                    'url': str(row.get('url') or ''),
                    'created_at': str(row.get('created_at') or ''),
                    'kind': str(row.get('kind') or ''),
                }
            )
            if len(latest_news_updates) >= 8:
                break

    company_updates: List[Dict[str, Any]] = []
    company_order = list(PARTY_PATTERNS.keys())
    stance_priority = {'market': 5, 'growth': 4, 'technical': 3, 'policy': 2, 'warning': 1}

    for company in company_order:
        pattern = PARTY_PATTERNS[company]
        best_row: Optional[Dict[str, Any]] = None
        best_tag = ''
        best_score = (-1, 0.0)

        for row in evidence_rows:
            text = row['text']
            if not pattern.search(text):
                continue
            tags = _detect_stance_tags(text)
            if not tags:
                tags = ['technical']
            dominant = max(tags, key=lambda tag: stance_priority.get(tag, 0))
            score = stance_priority.get(dominant, 0)
            if row.get('kind') == 'news':
                score += 1
            rank = (score, float(row.get('ts', 0.0)))
            if rank > best_score:
                best_score = rank
                best_row = row
                best_tag = dominant

        if best_row is None:
            continue

        company_updates.append(
            {
                'company': company,
                'update': _stance_text(best_tag),
                'evidence': _truncate(str(best_row.get('text') or ''), 170),
                'source': str(best_row.get('source') or ''),
                'url': str(best_row.get('url') or ''),
                'created_at': str(best_row.get('created_at') or ''),
            }
        )

    tech_developments_map: Dict[str, Dict[str, Any]] = {}
    for row in evidence_rows:
        text = row['text']
        for theme, pattern in CITY_PATTERNS.items():
            if not pattern.search(text):
                continue
            existing = tech_developments_map.get(theme)
            if not existing:
                tech_developments_map[theme] = {
                    'theme': theme,
                    'mentions': 1,
                    'last_ts': row['ts'],
                    'detail': _truncate(text, 170),
                    'source': row['source'],
                    'url': row['url'],
                    'created_at': row['created_at'],
                }
            else:
                existing['mentions'] += 1
                if row['ts'] >= existing['last_ts']:
                    existing['last_ts'] = row['ts']
                    existing['detail'] = _truncate(text, 170)
                    existing['source'] = row['source']
                    existing['url'] = row['url']
                    existing['created_at'] = row['created_at']

    tech_developments = sorted(
        tech_developments_map.values(),
        key=lambda row: (int(row.get('mentions', 0)), float(row.get('last_ts', 0.0))),
        reverse=True,
    )[:8]
    for row in tech_developments:
        row.pop('last_ts', None)

    funding_market_updates: List[Dict[str, Any]] = []
    funding_seen: set[Tuple[str, int, str]] = set()
    for row in evidence_rows:
        text = row['text']
        matches = _extract_casualty_mentions(text)

        for match in matches:
            key = (match['kind'], int(match['count']), text.lower())
            if key in funding_seen:
                continue
            funding_seen.add(key)
            funding_market_updates.append(
                {
                    'kind': match['kind'],
                    'count': int(match['count']),
                    'text': _truncate(text, 170),
                    'source': row['source'],
                    'url': row['url'],
                    'created_at': row['created_at'],
                    'ts': row['ts'],
                }
            )
        if matches:
            continue

        if FUNDING_MARKET_PATTERN.search(text):
            key = ('market', 0, text.lower())
            if key in funding_seen:
                continue
            funding_seen.add(key)
            funding_market_updates.append(
                {
                    'kind': 'market',
                    'count': 0,
                    'text': _truncate(text, 170),
                    'source': row['source'],
                    'url': row['url'],
                    'created_at': row['created_at'],
                    'ts': row['ts'],
                }
            )

    funding_market_updates.sort(
        key=lambda row: (float(row.get('ts', 0.0)), int(row.get('count', 0))),
        reverse=True,
    )
    for row in funding_market_updates:
        row.pop('ts', None)

    money_values = [int(row['count']) for row in funding_market_updates if row.get('kind') == 'money']
    percent_values = [int(row['count']) for row in funding_market_updates if row.get('kind') == 'percent']
    process_node_values = [int(row['count']) for row in funding_market_updates if row.get('kind') == 'process-node']

    funding_market_signals = {
        'largest_funding_mention_usd': max(money_values) if money_values else None,
        'largest_market_percent_mention': max(percent_values) if percent_values else None,
        'smallest_process_node_nm': min(process_node_values) if process_node_values else None,
        'recent_updates': funding_market_updates[:8],
        'note': 'Auto-extracted funding and market signals from current headlines/posts; values may overlap across reports.',
    }

    if latest_news_updates:
        latest_text = f"Latest highlight: {latest_news_updates[0]['text']}"
    else:
        latest_text = 'Latest highlight: limited signal extracted from current feed items.'

    if company_updates:
        lead_companies = ', '.join(row['company'] for row in company_updates[:4])
        company_text = f'Key company updates center on {lead_companies}.'
    else:
        company_text = 'No single company cluster dominates current reporting.'

    if tech_developments:
        lead_themes = ', '.join(row['theme'] for row in tech_developments[:3])
        tech_text = f'Technology development focus: {lead_themes}.'
    else:
        tech_text = 'Technology development themes are currently diffuse.'

    if funding_market_signals['largest_funding_mention_usd'] is not None:
        market_text = (
            f"Largest funding/valuation figure mentioned: "
            f"${int(funding_market_signals['largest_funding_mention_usd']):,}."
        )
    elif funding_market_signals['largest_market_percent_mention'] is not None:
        market_text = (
            f"Largest market percentage mention: "
            f"{int(funding_market_signals['largest_market_percent_mention']):,}%."
        )
    else:
        market_text = 'Funding and market metrics are mostly qualitative in current items.'

    overview = f'{latest_text} {company_text} {tech_text} {market_text}'

    x_posts_sorted = sorted(x_posts, key=lambda row: _parse_iso_to_ts(row.get('created_at')), reverse=True)
    top_x_by_engagement = sorted(
        x_posts,
        key=lambda row: (_post_engagement_score(row), _parse_iso_to_ts(row.get('created_at'))),
        reverse=True,
    )

    top_keywords = [
        {'keyword': keyword, 'count': count}
        for keyword, count in keyword_counter.most_common(8)
        if count > 0
    ]
    top_sources = [
        {'source': source, 'count': count}
        for source, count in source_counter.most_common(5)
    ]

    top_articles = [
        {
            'title': str(article.get('title') or ''),
            'source': str(article.get('source') or ''),
            'url': str(article.get('url') or ''),
            'published_at': str(article.get('published_at') or ''),
        }
        for article in articles[:5]
    ]

    top_x_posts = []
    seen_x_urls: set[str] = set()
    for post in top_x_by_engagement:
        url = post.get('url') or ''
        if not url or url in seen_x_urls:
            continue
        seen_x_urls.add(url)
        top_x_posts.append(
            {
                'handle': post['handle'],
                'label': post['label'],
                'text': _truncate(post.get('text', ''), 160),
                'url': url,
                'created_at': post.get('created_at', ''),
                'like_count': post.get('like_count'),
                'retweet_count': post.get('retweet_count'),
                'reply_count': post.get('reply_count'),
                'quote_count': post.get('quote_count'),
                'engagement': _post_engagement_score(post),
            }
        )
        if len(top_x_posts) >= 5:
            break

    total_posts = len(x_posts)
    accounts_with_posts = len(x_source_count)
    unique_news_sources = len(source_counter)

    conflict_trajectory = [dict(row) for row in latest_news_updates[:6]]
    party_attitudes = [
        {
            'party': row.get('company', ''),
            'attitude': row.get('update', ''),
            'evidence': row.get('evidence', ''),
            'source': row.get('source', ''),
            'url': row.get('url', ''),
            'created_at': row.get('created_at', ''),
        }
        for row in company_updates[:8]
    ]
    impacted_cities = [
        {
            'city': row.get('theme', ''),
            'mentions': int(row.get('mentions', 0)),
            'last_mention': row.get('detail', ''),
            'source': row.get('source', ''),
            'url': row.get('url', ''),
            'created_at': row.get('created_at', ''),
        }
        for row in tech_developments[:8]
    ]
    casualty_summary = {
        'largest_money_mention': funding_market_signals.get('largest_funding_mention_usd'),
        'largest_percent_mention': funding_market_signals.get('largest_market_percent_mention'),
        'smallest_process_node_nm': funding_market_signals.get('smallest_process_node_nm'),
        'recent_reports': funding_market_updates[:8],
        'note': str(funding_market_signals.get('note') or ''),
    }

    bullets: List[str] = [row['text'] for row in latest_news_updates[:6]]

    payload = {
        'updated_at': time.time(),
        'summary': overview,
        'overview': overview,
        'bullets': bullets[:6],
        'latest_news_updates': latest_news_updates[:6],
        'company_updates': company_updates[:8],
        'tech_developments': tech_developments[:8],
        'funding_market_updates': funding_market_updates[:8],
        'funding_market_signals': funding_market_signals,
        'conflict_trajectory': conflict_trajectory[:6],
        'party_attitudes': party_attitudes,
        'impacted_cities': impacted_cities,
        'casualty_reports': casualty_summary,
        'stats': {
            'articles': len(articles),
            'news_sources': unique_news_sources,
            'x_posts': total_posts,
            'x_accounts_with_posts': accounts_with_posts,
            'tracked_x_accounts': len(active_x_accounts),
        },
        'hot_keywords': top_keywords,
        'top_sources': top_sources,
        'top_articles': top_articles,
        'top_x_posts': top_x_posts,
        'latest_x_posts': x_posts_sorted[:5],
        'source_errors': source_errors,
        'x_errors': x_errors[:12],
        'x_modes': dict(x_mode_counter),
    }
    payload['zh'] = _build_summary_zh(payload)

    with _summary_lock:
        _summary_cache[cache_key] = {'updated_at': time.time(), 'payload': dict(payload)}

    return payload


def _cleanup_markdown_line(line: str) -> str:
    cleaned = line
    cleaned = re.sub(r'\[!\[.*?\]\(.*?\)\]\(.*?\)', ' ', cleaned)
    cleaned = re.sub(r'!\[.*?\]\(.*?\)', ' ', cleaned)
    cleaned = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'\1 \2', cleaned)
    cleaned = re.sub(r'[#*_`]+', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def _extract_status_urls(text: str) -> List[str]:
    return re.findall(r'https://x\.com/[A-Za-z0-9_]+/status/\d+', text or '')


def _extract_media_urls(text: str) -> List[str]:
    # Supports regular photos and video preview thumbnails from pbs.twimg.com.
    candidates = re.findall(r'https://pbs\.twimg\.com/[^\s\)]+', text or '')
    urls: List[str] = []
    seen: set[str] = set()
    for url in candidates:
        if '/profile_images/' in url:
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_urls_from_text(text: str) -> List[str]:
    candidates = re.findall(r'https?://[^\s<>\]\)"]+', text or '')
    urls: List[str] = []
    seen: set[str] = set()
    for raw in candidates:
        cleaned = raw.rstrip('.,;:!?)\'"')
        if not cleaned:
            continue
        parsed = urllib.parse.urlparse(cleaned)
        if not parsed.scheme or not parsed.netloc:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        urls.append(cleaned)
    return urls


def _normalize_url_for_match(url: str) -> str:
    parsed = urllib.parse.urlparse(str(url or '').strip())
    if not parsed.scheme or not parsed.netloc:
        return ''
    host = parsed.netloc.lower()
    if host.startswith('www.'):
        host = host[4:]

    path = parsed.path or '/'
    if path != '/' and path.endswith('/'):
        path = path.rstrip('/')

    tracking_keys = {
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_term',
        'utm_content',
        'utm_id',
        'gclid',
        'fbclid',
        'ref',
        'ref_src',
        'source',
    }
    kept_query: List[Tuple[str, str]] = []
    for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=False):
        if key.lower() in tracking_keys:
            continue
        kept_query.append((key, value))
    query = urllib.parse.urlencode(kept_query, doseq=True)

    return urllib.parse.urlunparse((parsed.scheme.lower(), host, path, '', query, ''))


def _is_x_domain(host: str) -> bool:
    normalized = host.lower().strip()
    if normalized.startswith('www.'):
        normalized = normalized[4:]
    return normalized in {'x.com', 'twitter.com', 'mobile.x.com', 'mobile.twitter.com'}


def _looks_like_x_status_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(str(url or '').strip())
    if not _is_x_domain(parsed.netloc):
        return False
    return bool(re.search(r'/[A-Za-z0-9_]+/status/\d+', parsed.path or ''))


def _canonicalize_x_status_url(url: str) -> str:
    parsed = urllib.parse.urlparse(str(url or '').strip())
    if not _is_x_domain(parsed.netloc):
        return str(url or '').strip()
    match = re.search(r'/([A-Za-z0-9_]+)/status/(\d+)', parsed.path or '')
    if not match:
        return str(url or '').strip()
    return f'https://x.com/{match.group(1)}/status/{match.group(2)}'


def _build_news_url_index(articles: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for article in articles:
        raw_url = str(article.get('url') or '').strip()
        if not raw_url:
            continue
        normalized = _normalize_url_for_match(raw_url)
        index[raw_url] = article
        if normalized:
            index[normalized] = article
    return index


def _build_reading_watch(force: bool = False, limit: int = READING_WATCH_DEFAULT_LIMIT) -> Dict[str, Any]:
    per_account_limit = max(1, min(int(limit), 12))
    recency_cutoff_ts = time.time() - max(LOOKBACK_DAYS, 14) * 86400
    news_payload = refresh_cache(force=False)
    articles = list(news_payload.get('news') or [])
    article_index = _build_news_url_index(articles)
    trusted_article_domains: set[str] = set()
    for article in articles:
        raw_url = str(article.get('url') or '').strip()
        if not raw_url:
            continue
        domain = urllib.parse.urlparse(raw_url).netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        if domain:
            trusted_article_domains.add(domain)

    active_map = {item['handle'].lower(): item for item in _active_x_accounts()}
    account_targets: List[Dict[str, str]] = []
    for account in READING_WATCH_ACCOUNTS:
        configured = active_map.get(str(account['handle']).lower())
        if configured:
            account_targets.append(
                {
                    'label': str(configured.get('label') or account['label']),
                    'handle': str(configured.get('handle') or account['handle']),
                }
            )
        else:
            account_targets.append({'label': account['label'], 'handle': account['handle']})

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for account in account_targets:
        handle = str(account['handle']).strip().lstrip('@')
        label = str(account.get('label') or handle)
        handle_count = 0
        seen_links: set[str] = set()

        try:
            data = fetch_x_posts_for_handle(
                handle=handle,
                limit=max(per_account_limit * 2, per_account_limit),
                force=force,
                allow_reader_fallback=True,
            )
        except Exception as exc:
            errors.append(f'@{handle}: {exc}')
            continue

        error = str(data.get('error') or '').strip()
        if error:
            errors.append(f'@{handle}: {error}')

        posts = data.get('posts') or []
        for post in posts:
            if handle_count >= per_account_limit:
                break

            created_at = str(post.get('created_at') or '')
            created_ts = _parse_iso_to_ts(created_at)
            if created_ts and created_ts < recency_cutoff_ts:
                continue

            post_text = str(post.get('text') or '').strip()
            post_url = str(post.get('url') or f'https://x.com/{handle}').strip()
            post_url_norm = _normalize_url_for_match(post_url)
            urls_in_post = _extract_urls_from_text(post_text)

            appended_for_post = False
            for shared_url in urls_in_post:
                if handle_count >= per_account_limit:
                    break

                if _looks_like_x_status_url(shared_url):
                    shared_url = _canonicalize_x_status_url(shared_url)
                shared_norm = _normalize_url_for_match(shared_url)
                if not shared_norm:
                    continue
                if shared_norm == post_url_norm and not _looks_like_x_status_url(shared_url):
                    continue

                parsed = urllib.parse.urlparse(shared_url)
                domain = parsed.netloc.lower()
                if domain.startswith('www.'):
                    domain = domain[4:]

                if domain in {'t.co', 'pbs.twimg.com', 'video.twimg.com'}:
                    continue
                if _is_x_domain(domain) and not _looks_like_x_status_url(shared_url):
                    continue
                if shared_norm in seen_links:
                    continue
                seen_links.add(shared_norm)

                article = article_index.get(shared_url) or article_index.get(shared_norm) or {}
                read_type = 'x_post' if _looks_like_x_status_url(shared_url) else 'article'
                if read_type == 'x_post':
                    status_match = re.search(r'/status/(\d+)', shared_url)
                    if not status_match:
                        continue
                    try:
                        status_id = int(str(status_match.group(1)))
                    except ValueError:
                        continue
                    if status_id < READING_WATCH_MIN_STATUS_ID:
                        continue
                elif not article and domain not in trusted_article_domains:
                    continue
                title = str(article.get('title') or '')
                summary = str(article.get('summary') or '')

                if not title:
                    if read_type == 'x_post':
                        title = f'X post shared by @{handle}'
                    else:
                        title = f'Link shared by @{handle}'
                if not summary:
                    summary = _truncate(post_text, 180)

                items.append(
                    {
                        'by_handle': handle,
                        'by_label': label,
                        'created_at': created_at,
                        'post_url': post_url,
                        'post_text': _truncate(post_text, 210),
                        'read_url': shared_url,
                        'read_domain': domain,
                        'read_type': read_type,
                        'title': _truncate(title, 170),
                        'summary': _truncate(summary, 220),
                        'like_count': _to_int_or_none(post.get('like_count')),
                        'retweet_count': _to_int_or_none(post.get('retweet_count')),
                        'reply_count': _to_int_or_none(post.get('reply_count')),
                        'quote_count': _to_int_or_none(post.get('quote_count')),
                    }
                )
                handle_count += 1
                appended_for_post = True

            if handle_count >= per_account_limit:
                break

            if appended_for_post:
                continue

            lowered = post_text.lower()
            if not (lowered.startswith('rt @') or 'repost' in lowered or 'quote' in lowered):
                continue
            if post_url_norm and post_url_norm in seen_links:
                continue
            if post_url_norm:
                seen_links.add(post_url_norm)

            items.append(
                {
                    'by_handle': handle,
                    'by_label': label,
                    'created_at': created_at,
                    'post_url': post_url,
                    'post_text': _truncate(post_text, 210),
                    'read_url': post_url,
                    'read_domain': 'x.com',
                    'read_type': 'x_post',
                    'title': f'Repost/quote activity by @{handle}',
                    'summary': _truncate(post_text, 220),
                    'like_count': _to_int_or_none(post.get('like_count')),
                    'retweet_count': _to_int_or_none(post.get('retweet_count')),
                    'reply_count': _to_int_or_none(post.get('reply_count')),
                    'quote_count': _to_int_or_none(post.get('quote_count')),
                }
            )
            handle_count += 1

        if handle_count == 0:
            errors.append(f'@{handle}: no recent shared links/reposts detected in current fetch.')

    items.sort(key=lambda row: _parse_iso_to_ts(row.get('created_at')), reverse=True)

    if items and SUMMARY_TRANSLATION_ENABLED:
        texts: List[str] = []
        for item in items:
            texts.append(str(item.get('title') or ''))
            texts.append(str(item.get('summary') or ''))
        translated = _translate_batch_to_zh(texts)
        for item in items:
            title = str(item.get('title') or '')
            summary = str(item.get('summary') or '')
            item['title_zh'] = translated.get(_normalize_translation_text(title), '')
            item['summary_zh'] = translated.get(_normalize_translation_text(summary), '')
    else:
        for item in items:
            item['title_zh'] = ''
            item['summary_zh'] = ''

    return {
        'updated_at': time.time(),
        'accounts': account_targets,
        'items': items[: per_account_limit * max(1, len(account_targets))],
        'errors': errors[:8],
    }


def _parse_reader_published_time(raw_text: str) -> str:
    match = re.search(r'^Published Time:\s*(.+)$', raw_text, flags=re.MULTILINE)
    if not match:
        return ''
    value = match.group(1).strip()
    dt = parse_datetime(value)
    if dt:
        return dt.isoformat()
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat()
    except (TypeError, ValueError):
        return ''


def fetch_x_posts_via_reader(handle: str, limit: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    normalized = handle.strip().lstrip('@').lower()
    url = f'https://r.jina.ai/http://x.com/{normalized}'

    try:
        response = session.get(url, timeout=FETCH_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as exc:
        return [], f'Reader fallback request failed: {exc}'

    body = response.text or ''
    if not body.strip():
        return [], 'Reader fallback returned empty content.'

    published_at = _parse_reader_published_time(body)
    lines = body.splitlines()

    start_index = 0
    for idx, line in enumerate(lines):
        low = line.lower()
        if "’s posts" in low or "'s posts" in low:
            start_index = idx + 1
            break

    candidate_lines = lines[start_index:] if start_index < len(lines) else lines
    dedupe: set[str] = set()
    posts: List[Dict[str, Any]] = []

    for raw in candidate_lines:
        line = raw.strip()
        if not line:
            continue

        status_urls_in_line = _extract_status_urls(line)
        media_urls_in_line = _extract_media_urls(line)

        # Associate media/image lines with the nearest previously parsed post.
        if media_urls_in_line:
            if posts:
                target = posts[-1]
                existing = target.get('media_urls', [])
                existing_set = set(existing)
                for media_url in media_urls_in_line:
                    if media_url not in existing_set:
                        existing.append(media_url)
                        existing_set.add(media_url)
                target['media_urls'] = existing
                if existing and not target.get('media_url'):
                    target['media_url'] = existing[0]
                if status_urls_in_line and (
                    not target.get('url') or target['url'].rstrip('/').endswith(f'/{normalized}')
                ):
                    target['url'] = status_urls_in_line[0]
            continue

        lower = line.lower()
        if lower.startswith(('title:', 'url source:', 'published time:', 'markdown content:')):
            continue
        if line.startswith('---'):
            continue
        if line in ('Pinned', 'Quote'):
            continue
        if line.startswith('![') or line.startswith('[!['):
            continue
        if re.fullmatch(r'@[\w_]+', line):
            continue
        if re.fullmatch(r'\d+[hm]', lower):
            continue
        if re.fullmatch(r'\d{1,2}:\d{2}', line):
            continue
        if lower.endswith('posts'):
            continue

        cleaned = _cleanup_markdown_line(line)
        if len(cleaned) < 28:
            continue

        key = cleaned.lower()
        if key in dedupe:
            continue
        dedupe.add(key)

        status_match = re.search(r'https://x\.com/[A-Za-z0-9_]+/status/\d+', cleaned)
        post_url = status_match.group(0) if status_match else (
            status_urls_in_line[0] if status_urls_in_line else f'https://x.com/{normalized}'
        )

        pseudo_id = hashlib.sha1(f'{normalized}:{cleaned}'.encode('utf-8')).hexdigest()[:16]
        posts.append(
            {
                'id': pseudo_id,
                'text': cleaned,
                'created_at': published_at,
                'url': post_url,
                'media_url': '',
                'media_urls': [],
                'reply_count': None,
                'retweet_count': None,
                'like_count': None,
                'quote_count': None,
            }
        )

        if len(posts) >= limit:
            break

    if not posts:
        return [], 'Reader fallback parsed no post text.'

    return posts, None


def fetch_x_posts_for_handle(
    handle: str,
    limit: int,
    force: bool = False,
    allow_reader_fallback: bool = True,
) -> Dict[str, Any]:
    normalized = handle.strip().lstrip('@').lower()
    now = time.time()

    with _x_cache_lock:
        cached = _x_posts_cache.get(normalized)
        if cached and not force and (now - float(cached.get('updated_at', 0.0))) < X_CACHE_TTL_SECONDS:
            return {
                'updated_at': float(cached.get('updated_at', now)),
                'posts': list(cached.get('posts', []))[:limit],
                'error': cached.get('error'),
                'mode': str(cached.get('mode', 'unknown')),
            }

    if not X_BEARER_TOKEN:
        message = 'X API token is missing on server.'
        if X_READER_FALLBACK_ENABLED and allow_reader_fallback:
            fallback_posts, fallback_error = fetch_x_posts_via_reader(normalized, limit=limit)
            if fallback_posts:
                enriched = _enrich_posts_with_public_metrics(fallback_posts)
                mode = 'reader_fallback_enriched' if enriched > 0 else 'reader_fallback'
                fallback = {
                    'updated_at': time.time(),
                    'posts': fallback_posts,
                    'error': None,
                    'mode': mode,
                }
                with _x_cache_lock:
                    _x_posts_cache[normalized] = fallback
                return {
                    'updated_at': fallback['updated_at'],
                    'posts': list(fallback['posts'])[:limit],
                    'error': None,
                    'mode': mode,
                }

            if fallback_error:
                message = f'{message} Reader fallback failed: {fallback_error}'

        with _x_cache_lock:
            _x_posts_cache[normalized] = {
                'updated_at': now,
                'posts': [],
                'error': message,
                'mode': 'unavailable',
            }
        return {'updated_at': now, 'posts': [], 'error': message, 'mode': 'unavailable'}

    max_results = max(limit, X_DEFAULT_POST_LIMIT)
    max_results = max(5, min(max_results, 20))

    try:
        user_id = resolve_x_user_id(normalized)
        payload = call_x_api(
            f'/users/{user_id}/tweets',
            {
                'max_results': max_results,
                'exclude': 'retweets,replies',
                'tweet.fields': 'created_at,public_metrics,entities,attachments',
                'expansions': 'attachments.media_keys',
                'media.fields': 'type,url,preview_image_url',
            },
        )

        rows = payload.get('data') or []
        includes = payload.get('includes') or {}
        media_rows = includes.get('media') or []
        media_by_key: Dict[str, Dict[str, Any]] = {}
        for media in media_rows:
            key = str(media.get('media_key') or '').strip()
            if key:
                media_by_key[key] = media

        posts: List[Dict[str, Any]] = []
        for row in rows:
            post_id = str(row.get('id') or '').strip()
            if not post_id:
                continue

            metrics = row.get('public_metrics') or {}
            text = expand_x_urls(str(row.get('text') or ''), row.get('entities') or {})
            attachments = row.get('attachments') or {}
            media_keys = attachments.get('media_keys') or []
            media_urls: List[str] = []
            media_types: List[str] = []
            seen_media: set[str] = set()
            for media_key in media_keys:
                media = media_by_key.get(str(media_key))
                if not media:
                    continue
                media_type = str(media.get('type') or '').strip()
                if media_type:
                    media_types.append(media_type)
                media_url = str(media.get('url') or media.get('preview_image_url') or '').strip()
                if media_url and media_url not in seen_media:
                    media_urls.append(media_url)
                    seen_media.add(media_url)

            posts.append(
                {
                    'id': post_id,
                    'text': text,
                    'created_at': str(row.get('created_at') or ''),
                    'url': f'https://x.com/{normalized}/status/{post_id}',
                    'media_url': media_urls[0] if media_urls else '',
                    'media_urls': media_urls,
                    'media_type': media_types[0] if media_types else '',
                    'reply_count': int(metrics.get('reply_count', 0)),
                    'retweet_count': int(metrics.get('retweet_count', 0)),
                    'like_count': int(metrics.get('like_count', 0)),
                    'quote_count': int(metrics.get('quote_count', 0)),
                }
            )

        refreshed = {'updated_at': time.time(), 'posts': posts, 'error': None, 'mode': 'x_api'}
        with _x_cache_lock:
            _x_posts_cache[normalized] = refreshed

        return {
            'updated_at': refreshed['updated_at'],
            'posts': list(refreshed['posts'])[:limit],
            'error': None,
            'mode': 'x_api',
        }
    except Exception as exc:
        error = str(exc)

        if X_READER_FALLBACK_ENABLED and allow_reader_fallback:
            fallback_posts, fallback_error = fetch_x_posts_via_reader(normalized, limit=limit)
            if fallback_posts:
                enriched = _enrich_posts_with_public_metrics(fallback_posts)
                mode = 'reader_fallback_enriched' if enriched > 0 else 'reader_fallback'
                warning = f'X API failed ({error}). Using reader fallback.'
                failed = {
                    'updated_at': time.time(),
                    'posts': fallback_posts,
                    'error': warning,
                    'mode': mode,
                }
                with _x_cache_lock:
                    _x_posts_cache[normalized] = failed
                return failed
            if fallback_error:
                error = f'{error} Reader fallback failed: {fallback_error}'

        failed = {'updated_at': time.time(), 'posts': [], 'error': error, 'mode': 'unavailable'}
        with _x_cache_lock:
            _x_posts_cache[normalized] = failed
        return failed


def refresh_cache(force: bool = False) -> Dict[str, Any]:
    now = time.time()

    with _cache_lock:
        age = now - _cache['updated_at']
        has_data = bool(_cache['news'])
        if not force and has_data and age < CACHE_TTL_SECONDS:
            return {
                'updated_at': _cache['updated_at'],
                'news': list(_cache['news']),
                'source_errors': list(_cache['source_errors']),
            }

    all_items: List[Dict[str, Any]] = []
    errors: List[str] = []

    with ThreadPoolExecutor(max_workers=min(8, len(NEWS_SOURCES))) as pool:
        future_map = {pool.submit(fetch_source, source): source for source in NEWS_SOURCES}
        for future in as_completed(future_map):
            items, error = future.result()
            all_items.extend(items)
            if error:
                errors.append(error)

    deduped = dedupe_items(all_items)

    with _cache_lock:
        _cache['updated_at'] = time.time()
        _cache['news'] = deduped
        _cache['source_errors'] = errors
        return {
            'updated_at': _cache['updated_at'],
            'news': list(_cache['news']),
            'source_errors': list(_cache['source_errors']),
        }


@app.route('/')
def index() -> Any:
    return send_from_directory('.', 'index.html')


@app.route('/api/news', methods=['GET'])
def api_news() -> Any:
    force = request.args.get('force', '0').lower() in ('1', 'true', 'yes')
    q = request.args.get('q', '').strip().lower()
    source = request.args.get('source', 'all').strip().lower()
    translate = request.args.get('translate', '1').lower() in ('1', 'true', 'yes')

    try:
        limit = int(request.args.get('limit', '120'))
    except ValueError:
        limit = 120

    limit = max(1, min(limit, 300))

    payload = refresh_cache(force=force)
    articles = payload['news']

    if source and source != 'all':
        articles = [item for item in articles if item['source'].lower() == source]

    if q:
        articles = [
            item
            for item in articles
            if q in item['title'].lower()
            or q in item['summary'].lower()
            or q in ' '.join(item['matched_keywords']).lower()
        ]

    articles = articles[:limit]
    translated_articles = _add_news_translations(articles) if translate else [dict(item) for item in articles]

    return jsonify(
        {
            'status': 'ok',
            'count': len(translated_articles),
            'updated_at': datetime.fromtimestamp(payload['updated_at'], tz=timezone.utc).isoformat(),
            'source_errors': payload['source_errors'],
            'news': translated_articles,
            'translation_enabled': bool(translate and SUMMARY_TRANSLATION_ENABLED),
        }
    )


@app.route('/api/sources', methods=['GET'])
def api_sources() -> Any:
    return jsonify({'status': 'ok', 'sources': NEWS_SOURCES})


@app.route('/api/x-accounts', methods=['GET'])
def api_x_accounts() -> Any:
    accounts = []
    for account in _active_x_accounts():
        handle = account['handle']
        accounts.append(
            {
                'label': account['label'],
                'handle': handle,
                'tag': account['tag'],
                'x_url': account.get('x_url', f'https://x.com/{handle}'),
                'embed_url': account.get('embed_url', f'https://twitter.com/{handle}'),
            }
        )

    return jsonify({'status': 'ok', 'accounts': accounts})


@app.route('/api/x-posts', methods=['GET'])
def api_x_posts() -> Any:
    handle = request.args.get('handle', '').strip().lstrip('@')
    if not handle:
        return jsonify({'status': 'error', 'message': 'Query param "handle" is required'}), 400

    known_map = {item['handle'].lower(): item for item in _active_x_accounts()}
    account = known_map.get(handle.lower())
    if not account:
        return jsonify({'status': 'error', 'message': f'Unknown tracked account: {handle}'}), 404

    force = request.args.get('force', '0').lower() in ('1', 'true', 'yes')
    try:
        limit = int(request.args.get('limit', str(X_DEFAULT_POST_LIMIT)))
    except ValueError:
        limit = X_DEFAULT_POST_LIMIT
    limit = max(1, min(limit, 20))
    translate = request.args.get('translate', '1').lower() in ('1', 'true', 'yes')

    allow_reader_fallback = not bool(account.get('disable_reader_fallback', False))
    data = fetch_x_posts_for_handle(
        account['handle'],
        limit=limit,
        force=force,
        allow_reader_fallback=allow_reader_fallback,
    )
    translated_posts = _add_x_post_translations(data['posts']) if translate else [dict(item) for item in data['posts']]

    return jsonify(
        {
            'status': 'ok',
            'x_api_enabled': bool(X_BEARER_TOKEN),
            'x_reader_fallback_enabled': X_READER_FALLBACK_ENABLED,
            'translation_enabled': bool(translate and SUMMARY_TRANSLATION_ENABLED),
            'account': {
                'label': account['label'],
                'handle': account['handle'],
                'tag': account['tag'],
                'x_url': account.get('x_url', f"https://x.com/{account['handle']}"),
            },
            'count': len(translated_posts),
            'updated_at': datetime.fromtimestamp(float(data['updated_at']), tz=timezone.utc).isoformat(),
            'error': data.get('error'),
            'mode': data.get('mode', 'unknown'),
            'posts': translated_posts,
        }
    )


@app.route('/api/reading-watch', methods=['GET'])
def api_reading_watch() -> Any:
    force = request.args.get('force', '0').lower() in ('1', 'true', 'yes')
    try:
        limit = int(request.args.get('limit', str(READING_WATCH_DEFAULT_LIMIT)))
    except ValueError:
        limit = READING_WATCH_DEFAULT_LIMIT
    limit = max(1, min(limit, 12))

    payload = _build_reading_watch(force=force, limit=limit)
    return jsonify(
        {
            'status': 'ok',
            'updated_at': datetime.fromtimestamp(float(payload['updated_at']), tz=timezone.utc).isoformat(),
            'accounts': payload.get('accounts', []),
            'count': len(payload.get('items') or []),
            'items': payload.get('items', []),
            'errors': payload.get('errors', []),
        }
    )


@app.route('/api/summary', methods=['GET'])
def api_summary() -> Any:
    force = request.args.get('force', '0').lower() in ('1', 'true', 'yes')
    try:
        x_limit = int(request.args.get('x_limit', str(SUMMARY_DEFAULT_X_LIMIT)))
    except ValueError:
        x_limit = SUMMARY_DEFAULT_X_LIMIT
    x_limit = max(1, min(x_limit, 10))

    payload = _build_realtime_summary(force=force, x_limit=x_limit)

    return jsonify(
        {
            'status': 'ok',
            'updated_at': datetime.fromtimestamp(float(payload['updated_at']), tz=timezone.utc).isoformat(),
            'summary': payload.get('summary', ''),
            'overview': payload.get('overview', payload.get('summary', '')),
            'bullets': payload.get('bullets', []),
            'latest_news_updates': payload.get('latest_news_updates', []),
            'company_updates': payload.get('company_updates', []),
            'tech_developments': payload.get('tech_developments', []),
            'funding_market_updates': payload.get('funding_market_updates', []),
            'funding_market_signals': payload.get('funding_market_signals', {}),
            'conflict_trajectory': payload.get('conflict_trajectory', []),
            'party_attitudes': payload.get('party_attitudes', []),
            'impacted_cities': payload.get('impacted_cities', []),
            'casualty_reports': payload.get('casualty_reports', {}),
            'stats': payload.get('stats', {}),
            'hot_keywords': payload.get('hot_keywords', []),
            'top_sources': payload.get('top_sources', []),
            'top_articles': payload.get('top_articles', []),
            'top_x_posts': payload.get('top_x_posts', []),
            'latest_x_posts': payload.get('latest_x_posts', []),
            'source_errors': payload.get('source_errors', []),
            'x_errors': payload.get('x_errors', []),
            'x_modes': payload.get('x_modes', {}),
            'zh': payload.get('zh', {}),
        }
    )


@app.route('/api/health', methods=['GET'])
def api_health() -> Any:
    return jsonify(
        {
            'status': 'ok',
            'service': 'AI Chip Tech NewsHub API',
            'cache_ttl_seconds': CACHE_TTL_SECONDS,
            'lookback_days': LOOKBACK_DAYS,
            'x_api_enabled': bool(X_BEARER_TOKEN),
            'x_reader_fallback_enabled': X_READER_FALLBACK_ENABLED,
            'x_cache_ttl_seconds': X_CACHE_TTL_SECONDS,
        }
    )


if __name__ == '__main__':
    port = int(os.environ.get('PORT', '5053'))

    print('=' * 62)
    print('AI Chip Tech NewsHub API starting')
    print('=' * 62)
    print(f'URL: http://0.0.0.0:{port}')
    print(f'News API: http://0.0.0.0:{port}/api/news')
    print(f'X Accounts: http://0.0.0.0:{port}/api/x-accounts')
    print(f'X Posts: http://0.0.0.0:{port}/api/x-posts?handle=OpenAI')
    print(f'Reading Watch: http://0.0.0.0:{port}/api/reading-watch')
    print('=' * 62)

    app.run(host='0.0.0.0', port=port, debug=False)
