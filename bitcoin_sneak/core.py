# -*- coding: utf-8 -*-

from contextlib import closing
from future.moves.urllib import request
import json
import codecs
import sqlite3

from . import util


class BitcoindConnection(object):
    def __init__(self, url, username, password):
        super(BitcoindConnection, self).__init__()
        self.id = 'bitcoin_sneak'
        self.url = url
        self.username = username
        self.password = password
        pm = request.HTTPPasswordMgrWithDefaultRealm()
        pm.add_password(None, url, username, password)
        request.install_opener(request.build_opener(
            request.HTTPHandler(), request.HTTPBasicAuthHandler(pm)
        ))

    def send_request(self, method, params=None):
        req = request.Request(self.url)
        data = {
            'jsonrpc': '1.0',
            'id': self.id,
            'method': method
        }
        if params is not None:
            data['params'] = params
        query = bytes(json.dumps(data), encoding='utf-8')
        with closing(request.urlopen(req, query)) as conn:
            return json.load(
                codecs.getreader('utf-8')(conn),
                parse_float=util.parse_value
            )['result']

    def get_txinfo(self, txid, detailed=True):
        return self.send_request(
            'getrawtransaction',
            [txid, 1 if detailed else 0]
        )

    def get_txioinfo(self, txid):
        res = self.get_txinfo(txid)
        for vin in res['vin']:
            if 'txid' in vin:
                tmp = self.get_txinfo(vin['txid'])
                if 'vin' in tmp:
                    tmp = list(filter(
                        lambda x: x['n'] == vin['vout'],
                        tmp['vout']
                    ))
                if len(tmp) > 0:
                    vin['prev_out'] = tmp[0]
        return res


class Database(object):
    @property
    def schema_version(self):
        return '0'

    def __init__(self, connection):
        super(Database, self).__init__()
        if isinstance(connection, sqlite3.Connection):
            self.connection = connection
        else:
            self.connection = sqlite3.Connection(connection)
        if not self.is_init():
            self.__init_tables()

    def __init_tables(self):
        cur = self.connection.cursor()
        cur.execute(
            'CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)'
        )
        cur.execute(
            'CREATE TABLE wallet (' +
            'id TEXT PRIMARY KEY, ' +
            'name TEXT'
            ')'
        )
        cur.execute(
            'CREATE TABLE addr (' +
            'address TEXT PRIMARY KEY, ' +
            'wallet_id TEXT'
            ')'
        )
        cur.execute(
            'CREATE TABLE tx (' +
            'id TEXT PRIMARY KEY, ' +
            'ts INTEGER' +
            ')'
        )
        cur.execute(
            'CREATE TABLE txio (' +
            'tx_id TEXT NOT NULL REFERENCES tx(id), ' +
            'addr_address TEXT NOT NULL REFERENCES addr(address), ' +
            'incoming INTEGER NOT NULL, ' +
            'value INTEGER, ' +
            'constraint txio_pkey ' +
            'PRIMARY KEY (tx_id, addr_address, incoming)'
            ')'
        )
        cur.execute(
            'CREATE INDEX txio_ix_addr_address_incoming ON ' +
            'txio (addr_address, incoming)'
        )
        cur.executemany(
            'INSERT INTO metadata (key, value) VALUES (?, ?)',
            (('schema_version', self.schema_version),
             ('initialized_at', util.now()))
        )
        self.connection.commit()

    def is_init(self):
        """return ``True`` if this object is initialized with current schema\
        and return ``False`` otherwise.
        """
        cur = self.connection.cursor()
        masterdata = list(cur.execute(
            'SELECT * FROM sqlite_master WHERE name="metadata"'
        ))
        if len(masterdata) == 0:
            return False
        schema_version = list(cur.execute(
            'SELECT value FROM metadata ' +
            'WHERE key="schema_version"'
        ))
        if len(schema_version) > 0 and \
                schema_version[0][0] == self.schema_version:
            return True
        return False

    def save_wallet(self, wallet_id, name=None, cur=None, commit=False):
        if cur is None:
            cur = self.connection.cursor()
        if cur.execute('SELECT * FROM wallet WHERE id=?',
                       (wallet_id, )).fetchone() is not None:
            cur.execute('UPDATE wallet SET name=? WHERE id=?',
                        (name, wallet_id))
        else:
            cur.execute('INSERT INTO wallet (id, name) VALUES (?, ?)',
                        (wallet_id, name))
        if commit:
            self.connection.commit()

    def save_addr(self, address, wallet_id, cur=None, commit=False):
        if cur is None:
            cur = self.connection.cursor()
        if cur.execute('SELECT * FROM addr WHERE address=?',
                       (address, )).fetchone() is not None:
            cur.execute('UPDATE addr SET wallet_id=? WHERE address=?',
                        (wallet_id, address))
        else:
            cur.execute('INSERT INTO addr (address, wallet_id) VALUES (?, ?)',
                        (address, wallet_id))
        if commit:
            self.connection.commit()

    def save_tx(self, txid, ts, cur=None, commit=False):
        if cur is None:
            cur = self.connection.cursor()
        if cur.execute('SELECT * FROM tx WHERE id=?',
                       (txid, )).fetchone() is not None:
            cur.execute('UPDATE tx SET ts=? WHERE id=?',
                        (ts, txid))
        else:
            cur.execute('INSERT INTO tx (id, ts) VALUES (?, ?)',
                        (txid, ts))
        if commit:
            self.connection.commit()

    def save_txio(self, txid, address, incoming, value,
                  cur=None, commit=False):
        if cur is None:
            cur = self.connection.cursor()
        if cur.execute(
                    'SELECT * FROM txio WHERE ' +
                    'tx_id=? AND addr_address=? AND incoming=?',
                    (txid, address, incoming)
                ).fetchone() is not None:
            cur.execute(
                'UPDATE txio SET value=? WHERE ' +
                'tx_id=? AND addr_address=? AND incoming=?',
                (value, txid, address, incoming)
            )
        else:
            cur.execute(
                'INSERT INTO txio (tx_id, addr_address, incoming, value) ' +
                'VALUES (?, ?, ?, ?)',
                (txid, address, incoming, value)
            )
        if commit:
            self.connection.commit()

    def save_txinfo(self, txinfo, cur=None, commit=False):
        if cur is None:
            cur = self.connection.cursor()
        wallets = []
        addresses = []
        for d in txinfo['incoming']:
            addr_data = cur.execute(
                'SELECT wallet_id FROM addr WHERE address=?',
                (d['address'], )
            ).fetchone()
            if addr_data is not None:
                wid = addr_data[0]
                wallets.append(wid)
                addresses += [d[0] for d in cur.execute(
                    'SELECT address FROM addr WHERE wallet_id=?',
                    (wid, )
                )]
            else:
                addresses.append(d['address'])
        if len(wallets) == 0:
            wid = util.gen_id()
        else:
            wid = wallets[0]
            self.save_wallet(wid, cur=cur)
        for addr in addresses:
            self.save_addr(addr, wid, cur=cur)
        for d in txinfo['outgoing']:
            if cur.execute(
                        'SELECT wallet_id FROM addr WHERE address=?',
                        (d['address'], )
                    ).fetchone() is None:
                wid = util.gen_id()
                self.save_wallet(wid, cur=cur)
                self.save_addr(d['address'], wid, cur=cur)
            self.save_txio(txinfo['txid'], d['address'], 0, d['value'])
        for d in txinfo['incoming']:
            self.save_txio(txinfo['txid'], d['address'], 1, d['value'])
        self.save_tx(txinfo['txid'], txinfo['ts'], cur)
        if commit:
            self.connection.commit()
