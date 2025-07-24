import pandas as pd
import json
import sys
import os
import numpy as np
from pathlib import Path
from urllib.parse import parse_qs, unquote_plus
import asyncio
import re
import miniparser

start_url = sys.argv[1] if len(sys.argv) > 1 else "https://transport.orgp.spb.ru"
output_file = start_url
if 'https://' in output_file:
    output_file = output_file.removeprefix('https://')
elif 'http://' in output_file:
    output_file = output_file.removeprefix('http://')
else: pass
output_file = output_file.replace('.', '_').replace('/', '-')
output_file = Path(f"../db/temp{output_file}.jsonl")

def parse_post_data(s):
    if not s or pd.isna(s):
        return {}
    decoded = unquote_plus(s)
    parsed = parse_qs(decoded)
    return {k: v[0] for k, v in parsed.items()}

def split_url_columns(df, columns):
    for col in columns:
        parts = df[col].apply(lambda x: re.split(r'/+', x.strip('/')) if pd.notna(x) and x.strip() != '' else [np.nan])
        max_len = parts.apply(len).max()
        parts_df = pd.DataFrame(
            parts.tolist(),
            columns=[f'{col}.{i+1}' for i in range(max_len)],
            index=df.index
        )
        df = pd.concat([df, parts_df], axis=1)
    return df

def load_df():
    data = []
    with open(f'{output_file}', 'r', encoding='utf-8') as f:
        count = 0
        for line in f:
            count+=1
            if not line.startswith("{"):
                print("💥 Corrupted line:", repr(line))
                continue
            data.append(json.loads(line))
    df = pd.DataFrame(data)
    cols_to_split = ['url']
    df = split_url_columns(df, cols_to_split).drop(columns=cols_to_split)
    df = df[(df['body'].apply(lambda x: bool(x))) & (df['status'] == 200)].drop(columns=['status'])
    # freq = df['url.5'].value_counts()
    # print(freq)
    return df

async def main():
    if os.path.exists(output_file):
        pass
    else: return
    df = load_df()
    
if __name__ == '__main__':
    asyncio.run(main())