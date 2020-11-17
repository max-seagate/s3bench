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


OBJECT_SIZE = 16 * 2 ** 20
PARAMS = {
    'compressionRatioPercent': ['0', '10', '20', '40', '60', '80', '100'],
    'dedupRatioPercent':       ['0', '10', '20', '40', '60', '80', '100'],
    'reductionBlockSize':      ['4Kb', '16Kb', '64Kb', '256Kb', '1Mb'],
    'dedupCortxUnitSize':      ['1Mb', '4Mb'],
    'objectSize':              [f'{str(OBJECT_SIZE)}b'],
}
SPLIT_SIZE = {'4Kb': 4096, '16Kb': 16384, '64Kb': 65536, '256Kb': 262144,
              '1Mb': 1048576}
COMPRESSOR = 'lz4'
HASH = 'sha1sum'
PATH = 'test'
W = 6
H = ['dedupRatioPercent', 'dedupCortxUnitSize']
V = ['compressionRatioPercent', 'reductionBlockSize']


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
              'results_compressed', 'results_dedup']:
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
        for d in ['results_compressed', 'results_dedup']:
            with open(os.path.join(PATH, d, c)) as f:
                files[d] = f.read()
        dedup = float(files['results_dedup'].split()[0]) / \
            float(int(files['results_dedup'].split()[1]))
        r[c] = {
            'compression': float(files['results_compressed']) / OBJECT_SIZE,
            'dedup': dedup,
        }
    HH = header(H)
    VH = header(V)
    print(HH)
    print(VH)
    for kind in ['compression', 'dedup']:
        print(kind)
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
