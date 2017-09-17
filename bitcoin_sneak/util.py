# -*- coding: utf-8 -*-

import time
from datetime import datetime

import shortuuid


shortuuid.set_alphabet(
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
)


def gen_id():
    return shortuuid.uuid()


def dt2ts(dt):
    return int(time.mktime(dt.timetuple()))


def ts2dt(ts):
    return datetime.fromtimestamp(ts)


def now():
    return dt2ts(datetime.now())


def parse_value(s):
    ss = s.split('.')
    ss[1] += '0' * (8 - len(ss[1]))
    return int(ss[0]) * 100000000 + int(ss[1])


def tidy_txinfo(data):
    res = dict()
    res['txid'] = data['txid']
    res['ts'] = data['time']
    incoming = {}
    outgoing = {}
    txtx = []
    for d in data['vin']:
        if 'prev_out' not in d:
            continue
        addr = ' '.join(d['prev_out']['scriptPubKey']['addresses'])
        if addr not in incoming:
            incoming[addr] = 0
        incoming[addr] += d['prev_out']['value']
        txtx.append({
            'address': addr, 'tx_id_from': d['txid'], 'tx_id_to': data['txid'],
            'amount': d['prev_out']['value'], 'n': d['vout']
        })
    for d in data['vout']:
        if d['value'] != 0:
            addr = ' '.join(d['scriptPubKey']['addresses'])
            if addr not in outgoing:
                outgoing[addr] = 0
            outgoing[addr] += d['value']
    res['incoming'] = [
        {'address': k, 'value': v} for (k, v) in incoming.items()
    ]
    res['outgoing'] = [
        {'address': k, 'value': v} for (k, v) in outgoing.items()
    ]
    res['txtx'] = txtx
    return res
