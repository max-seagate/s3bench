#!/usr/bin/env python


import os
import itertools
import multiprocessing

from plumbum import local  # type: ignore
from plumbum import FG

from typing import List
from typing import Dict
from typing import Tuple
from typing import Optional


PARAMS = {
    'compressionPercent': ['0', '10', '20', '40', '60', '80', '100'],
    'dedupPercent':       ['0', '10', '20', '40', '60', '80', '100'],
    'reductionBlockSize': ['4KiB', '16KiB', '64KiB', '256KiB', '1MiB'],
    'dedupCortxUnitSize': ['1MiB', '4MiB'],
    'objectSize':         ['16MiB'],
}
SPLIT_SIZE = {'4KiB': 4096, '16KiB': 16384, '64KiB': 65536, '256KiB': 262144,
              '1MiB': 1048576}
OBJECT_SIZE = {'16MiB': 16 * 2 ** 20}
HASH = 'sha1sum'
PATH = 'test'
W = 6
H = ['dedupPercent', 'dedupCortxUnitSize']
V = ['compressionPercent', 'reductionBlockSize']
KIND = {'compression': 'compression, %',
        'dedup': 'deduplication, %',
        'reduction': 'total reduction (dedup, then compression), %',
        'ideal': 'reduction : ideal_reduction, %. '
        'ideal_reduction = dedupPercent * compressionPercent',
        }


def combine(names: List[str]) -> List[Tuple[str, str, List[str]]]:
    c_name = [(f'{names[0]}-{v}', f'-{names[0]} {v}', [v])
              for v in PARAMS[names[0]]]
    if len(names) == 1:
        return c_name
    else:
        return [(f'{c}-{dc}', f'{cmd} {dcmd}', v + dv)
                for dc, dcmd, dv in combine(names[1:])
                for c, cmd, v in c_name]


def header(names: List[str]) -> List[List[Optional[str]]]:
    h: List[Optional[str]] = list(PARAMS[names[0]])
    if len(names) == 1:
        return [h]
    else:
        subh = header(names[1:])
        return [list(itertools.chain.from_iterable(
            [[v] + [None] * (len(subh[0]) - 1) for v in h]))] + \
            [sh * len(h) for sh in subh]


def get(combinations: List[Tuple[str, str, List[str]]],
        r: Dict[str, Dict[str, float]],
        val: Dict[str, str], kind: str) -> float:
    assert len(val.keys()) == len(PARAMS)
    keys = [c for c, cmd, v in combinations
            if val == {kk: vv for kk, vv in zip(PARAMS.keys(), v)}]
    assert len(keys) == 1, (val, keys)
    return r[keys[0]][kind]


def main() -> None:
    names = list(PARAMS.keys())
    combinations = combine(names)
    for d in ['params_s3bench', 'params_split',
              'results_compression', 'results_dedup',
              'results_reduction']:
        os.makedirs(os.path.join(PATH, d), exist_ok=True)
    for c, cmd, v in combinations:
        for d, s in [
                ('params_s3bench', cmd),
                ('params_split', '--bytes='
                 f'{SPLIT_SIZE[v[names.index("reductionBlockSize")]]}')]:
            with open(os.path.join(PATH, d, c), 'w') as f:
                f.write(s)

    CPUs = multiprocessing.cpu_count()
    (local['ls']['test/params_s3bench'] > "test/cases")()
    local['parallel'][f'--arg-file test/cases --bar --jobs {CPUs+1} '
                      './test-single.sh {}'.split()] & FG

    r = {}
    for c, cmd, v in combinations:
        files = {}
        for d in ['results_compression', 'results_dedup', 'results_reduction']:
            with open(os.path.join(PATH, d, c)) as f:
                files[d] = f.read()
        dedup = float(files['results_dedup'].split()[0]) / \
            float(int(files['results_dedup'].split()[1]))
        object_size = OBJECT_SIZE[v[names.index('objectSize')]]
        reduction = float(files['results_reduction']) / object_size
        ideal_reduction = float(v[names.index('compressionPercent')]) * \
            float(v[names.index('dedupPercent')]) / 10000.
        r[c] = {
            'compression': float(files['results_compression']) / object_size,
            'reduction': reduction,
            'ideal': reduction / ideal_reduction
            if ideal_reduction != 0. else float('Inf'),
            'dedup': dedup,
        }
    HH = header(H)
    VH = header(V)
    for kind in KIND.keys():
        print(KIND[kind])
        for hh in H:
            print(f'==> {hh}')
        for i, vh in enumerate(V):
            print(f'{"v":>{(i+1)*W}} {vh}')
        vv = {'objectSize': PARAMS['objectSize'][0]}
        for y in range(-len(HH), len(VH[0])):
            for x in range(-len(VH), len(HH[0])):
                # print(f'{x=} {y=}')
                if x < 0 and y < 0:
                    print(f'{"":>{W}}', end='')
                    continue
                if x < 0:
                    h = VH[x][y] if VH[x][y] is not None else ''
                    print(f'{h:>{W}}', end='')
                if y < 0:
                    h = HH[y][x] if HH[y][x] is not None else ''
                    print(f'{h:>{W}}', end='')
                if x >= 0 and y >= 0:
                    for i, name in enumerate(H):
                        t = HH[i][x]
                        if t is not None:
                            vv[name] = t
                    for i, name in enumerate(V):
                        t = VH[i][y]
                        if t is not None:
                            vv[name] = t
                    print(f'{get(combinations, r, vv, kind) * 100:>{W}.1f}',
                          end='')
                if x == len(HH[0]) - 1:
                    print()


if __name__ == '__main__':
    main()
