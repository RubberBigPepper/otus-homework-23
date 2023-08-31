#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
import typing as tp
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from enum import Enum


MAX_FILE_THREADS = 2  # максимальное число потоков перебора файлов
MAX_LINE_THREADS = 4  # максимальное число потоков перебора строк в одном файле
NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class HandleLineResult(Enum):
    SKIP = -1
    OK = 0
    ERROR = 1


def dot_rename(path: str):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line: str):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def make_reader(filename: str):
    return gzip.open(filename, "rt") if filename.lower().endswith(".gz") else open(filename)


def handle_line(line: str, options: tp.Any, device_memc: tp.Dict[str, tp.Any]) -> HandleLineResult:
    line = line.strip()
    if not line:
        return HandleLineResult.SKIP
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        return HandleLineResult.ERROR
    memc_addr = device_memc.get(appsinstalled.dev_type)
    if not memc_addr:
        logging.error("Unknown device type: %s" % appsinstalled.dev_type)
        return HandleLineResult.ERROR
    return HandleLineResult.OK if insert_appsinstalled(memc_addr, appsinstalled,
                                                       options.dry) else HandleLineResult.ERROR


def calculate_raiting(results: tp.List[HandleLineResult]) -> None:
    processed = errors = 0
    for result in results:
        if result == HandleLineResult.OK:
            processed += 1
        elif HandleLineResult.ERROR:
            errors += 1
    if not processed:
        return
    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))


def process_file(filename: str, options: tp.Any, device_memc: tp.Dict[str, tp.Any]) -> None:
    logging.info('Processing %s' % filename)
    with make_reader(filename) as fd:
        pool = ThreadPool(MAX_LINE_THREADS)
        results = pool.map(partial(handle_line, options=options, device_memc=device_memc), fd)
        pool.close()
        pool.join()

    dot_rename(filename)
    calculate_raiting(results)


def main(options: tp.Any, max_threads: int = MAX_FILE_THREADS):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    files_to_work = sorted(glob.iglob(options.pattern))
    pool = ThreadPool(max_threads)
    pool.map(partial(process_file, options=options, device_memc=device_memc), files_to_work)
    pool.close()
    pool.join()


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
