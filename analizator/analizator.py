import pandas as pd
import json
import sys
import os
import numpy as np
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote_plus, quote, urlencode, urlunparse
import asyncio
import re
import uuid
import parse
import miniparser

def parse_post_data(s):
    if not s or pd.isna(s):
        return {}
    decoded = unquote_plus(s)
    parsed = parse_qs(decoded)
    return {k: v[0] for k, v in parsed.items()}

def parse_url_to_dict(url: str):
    res = urlparse(url)
    return {
        "url.scheme": res.scheme,
        "url.netloc": res.netloc,
        "url.path": res.path,
        "url.query": parse_qs(res.query),
        "url.fragment": res.fragment
    }

def explore_apis(df: pd.DataFrame, desc: pd.DataFrame) -> pd.DataFrame:
    def check_multiples(arr, threshold=0.8):
        diffs = np.diff(arr)
        for d in diffs:
            if d == 0:
                continue
            mod_vals = arr % d
            valid_ratio = np.mean(mod_vals == 0)
            if valid_ratio >= threshold and d not in (1, -1):
                return True, d
        return False, None

    def check_arithmetic_progression(arr, threshold=0.8):
        diffs = np.diff(arr)
        if len(diffs) == 0:
            return False, None
        # Считаем частоту каждого значения
        counts = Counter(diffs)
        most_common_diff, count = counts.most_common(1)[0]
        valid_ratio = count / len(diffs)
        if valid_ratio >= threshold:
            return True, most_common_diff
        return False, None
    
    request_templates = []
    for i, api in desc.iterrows():
        segment:pd.Dataframe = df[df['general'] == api['general']]
        queries, request_bodies, paths = pd.Series(dtype=object), pd.Series(dtype=object), pd.DataFrame()

        if api['query_variants'] > 0:
            queries = segment['url.query']

        if api['body_variants'] > 0:
            request_bodies = segment['request_body']

        if api['path_variants'] > 0:
            path_cols = sorted([c for c in segment.columns if c.startswith('url.path.lvl')])
            paths:pd.DataFrame = segment[path_cols]
            paths = '/'+paths.fillna('').astype(str).agg(lambda x: '/'.join(quote(part.strip('/')) for part in x if part), axis=1)
        

        if paths.shape[0]>0:
            results = []
            for path in paths:
                result = parse.parse(api['general'], path)
                if result:
                    results.append(pd.Series(result.named))
            if results:
                results = pd.DataFrame(results)
                pathTemplate = {'base':api['general']}
                for col in results.columns:
                    values = results[col].astype(int).to_numpy()
                    min_, max_ = values.min(), values.max()
                    multiple_flag, divisor = check_multiples(values)
                    arithmetic_flag, step = check_arithmetic_progression(values)
                    if multiple_flag:
                        pathTemplate[col]=('*', divisor, min_, max_)
                    elif arithmetic_flag:
                        pathTemplate[col]=('+', step, min_, max_)

        if queries.shape[0]>0:
            queryTemplate = max(queries, key=len)
        else:
            queryTemplate = None
        if request_bodies.shape[0]:
            requestTemplate = max(request_bodies, key=len)
        else:
            requestTemplate = None
        typical_response = max(segment['response_body'], key=len)
        if isinstance(typical_response, dict):
            typical_response = [f'{key}: {type(value)}' for key, value in typical_response.items()]
            typical_response = ', '.join(typical_response)
        elif isinstance(typical_response, list):
            typical_response = typical_response[:3]
        full_request_template = {
            'method':api['method'],
            'path':urlunparse((segment.iloc[0]['url.scheme'], segment.iloc[0]['url.netloc'], pathTemplate['base'], '', '', '')),
            'template':pathTemplate,
            'query':queryTemplate,
            'request':requestTemplate,
            'typical_response':typical_response
        }
        request_templates.append(full_request_template)
    return request_templates

def find_apis(init_df: pd.DataFrame):
    def matches(path, template):
        rp, tp = path.strip('/').split('/'), template.strip('/').split('/')
        if len(rp) != len(tp): return False
        for r, t in zip(rp, tp):
            if t == '{int}' and not r.isdigit(): return False
            if t == '{float}':
                try: float(r)
                except: return False
            if t == '{uuid}':
                if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', r, re.I): return False
            elif t not in ('{int}', '{float}', '{uuid}') and r != t: return False
        return True
    df = init_df.copy()
    int_re = re.compile(r'^\d+$')
    float_re = re.compile(r'^\d+\.\d+$')
    uuid_re = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I
    )

    def segment_to_template(seg):
        if pd.isna(seg):
            return None
        seg = str(seg)
        if int_re.match(seg):
            return '{int}'
        if float_re.match(seg):
            return '{float}'
        if uuid_re.match(seg):
            return '{uuid}'
        try:
            uuid.UUID(seg)
            return '{uuid}'
        except:
            pass
        return seg

    def generalize_row(row):
        parts = []
        path_cols = sorted([c for c in row.index if c.startswith('url.path.lvl')])
        for col in path_cols:
            if row[col]:
                templ = segment_to_template(row[col])
                if templ is not None:
                    parts.append(templ)
        return '/' + '/'.join(parts)

    query_cols = [c for c in df.columns if c.startswith('url.query')]
    # Сериализуем весь dict из url.query в одну колонку
    df['query_serialized'] = df['url.query'].apply(
        lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
    )

    df['body_serialized'] = df['request_body'].apply(
        lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
    )

    # Группируем по method и url.path и считаем варианты
    grouped_original = df.groupby(['method', 'url.path']).agg(
        query_variants=('query_serialized', 'nunique'),
        body_variants=('body_serialized', 'nunique')
    ).reset_index()

    # Мержим обратно к df
    df = df.merge(grouped_original, on=['method', 'url.path'], how='left')

    # В choose_path можно оставить без изменений
    def choose_path(row):
        if row['query_variants'] > 1 or row['body_variants'] > 1:
            return row['url.path']
        else:
            return generalize_row(row)

    df['url.path.generalized'] = df.apply(choose_path, axis=1)
    df['method_path_generalized'] = df['method'] + ' ' + df['url.path.generalized']

    # В группировке для подсчётов уникальных вариантов query меняем на 1 столбец
    results = []
    for route, group in df.groupby('method_path_generalized'):
        # Сериализуем query_serialized (уже строка) — можно пропустить, просто dropna и drop_duplicates
        vals = group['query_serialized'].dropna().unique()
        # фильтруем пустые или пустые JSON-объекты
        filtered = [v for v in vals if v not in (None, '', '{}')]
        query_count = len(filtered)
        body_count = group['body_serialized'].dropna().nunique()

        resp_count = group['response_body'].apply(
            lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
        ).nunique()
        count = len(group)

        if query_count == 0 and body_count == 0:
            path_variants = group['url.path'].nunique()
        else:
            path_variants = 0

        results.append({
            "method_path": route,
            "responses_unique": resp_count,
            "query_variants": query_count,
            "body_variants": body_count,
            "count": count,
            "path_variants": path_variants
        })

    result_df = pd.DataFrame(results).sort_values(by='count', ascending=False).reset_index(drop=True)
    result_df[['method', 'general']] = result_df['method_path'].str.split(pat=' ', n=1, expand=True)
    result_df = result_df.drop(columns=['method_path'])
    for col in ['responses_unique', 'query_variants', 'body_variants', 'count', 'path_variants', 'general']:
        init_df[col] = None

    for _, row in result_df.iterrows():
        method = row['method']
        template = row['general']

        mask = (init_df['method'] == method) & init_df['url.path'].apply(lambda p: matches(p, template))
        for col in ['responses_unique', 'query_variants', 'body_variants', 'count', 'path_variants', 'general']:
            init_df.loc[mask, col] = row[col]
    return init_df, result_df

def execute_apis(templates:list):
    for t in templates:
        print(t)

def load_df(output_file):
    data = []
    with open(f'{output_file}', 'r', encoding='utf-8') as f:
        count = 0
        for line in f:
            count+=1
            if not line.startswith("{"):
                print("💥 Corrupted line:", repr(line)[:50])
                continue
            else:
                data.append(json.loads(line))
    df = pd.DataFrame(data)
    url_dicts = df["url"].apply(parse_url_to_dict)
    url_dicts = pd.json_normalize(url_dicts, max_level=0)
    df = pd.concat([df, url_dicts], axis=1)
    path_parts_df = df['url.path'].str.strip('/').str.split('/', expand=True)
    path_parts_df.columns = [f'url.path.lvl{i}' for i in path_parts_df.columns]
    df = pd.concat([df, path_parts_df], axis=1)
    df = df[(df['response_body'].apply(lambda x: bool(x))) & (df['status'] == 200)].drop(columns=['url'])
    return df

async def main(output_file):
    if os.path.exists(output_file):
        pass
    else: return
    df = load_df(output_file=output_file)
    df, desc = find_apis(df)
    templates = explore_apis(df, desc)
    execute_apis(templates)

if __name__ == '__main__':
    start_url = sys.argv[1] if len(sys.argv) > 1 else "https://transport.orgp.spb.ru"
    print("Start URL:", start_url)
    output_file = start_url
    if 'https://' in output_file:
        output_file = output_file.removeprefix('https://')
    elif 'http://' in output_file:
        output_file = output_file.removeprefix('http://')
    else: pass
    output_file = output_file.replace('.', '_').replace('/', '-')
    output_file = Path(f"./db/temp/{output_file}.jsonl")
    asyncio.run(main(output_file))