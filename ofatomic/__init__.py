from Crypto.Hash import SHA384
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from multiprocessing import Pool, cpu_count
from itertools import starmap
from os import makedirs
from os.path import exists, getsize
from sys import argv, exit
import csv
import urllib.request
from zstd import decompress
from pathlib import Path, PurePosixPath

global prefix
global keyfile
global signing
global hashing
global url
global cfg_write
global mpath
mpath = Path(__file__).parents[0]
signing = True
hashing = True
cfg_write = False
prefix = ''
keyfile = ''
nproc = cpu_count()
url = 'https://svn.openfortress.fun/launcher/files/'

def download_db(path):
    global keyfile
    req = url + "ofmanifest.csv"
    req_sig = url + "ofmanifest.sig"
    r = urllib.request.Request(req, headers={'User-Agent': 'Mozilla/5.0'})
    rs = urllib.request.Request(req_sig, headers={'User-Agent': 'Mozilla/5.0'})
    print("downloading db...")
    memfile = urllib.request.urlopen(r).read()
    sig = urllib.request.urlopen(rs).read()
    mfhash = SHA384.new(memfile)
    key = RSA.import_key(open(keyfile).read())
    pkcs1_15.new(key).verify(mfhash, sig)
    makedirs(path, exist_ok=True)
    f = open(path / "ofmanifest.csv", 'wb')
    f.write(memfile)
    f.close()
    print("done!")


def download_file_multi(path, fhash, sig, prefix, publickey, conn_l,upd):
    filename = Path(path)
    req = url + str(PurePosixPath(filename))
    path = prefix / filename
    print(req)
    try:
        r = urllib.request.Request(req, headers={'User-Agent': 'Mozilla/5.0'})
        u = urllib.request.urlopen(r)
    except ConnectionResetError:
        print("Timed out! you're going to have to redownload...")
        return 1
    if str(filename.parents[0]) != '.':
        spath = path.parents[0]
        makedirs(spath, exist_ok=True)
    memfile = u.read()
    u.close()
    if hashing:
        if memfile:
            memfile = decompress(memfile)
        new_hash = SHA384.new(memfile)
        if new_hash.hexdigest() != fhash:
            raise ArithmeticError("HASH INVALID for file {}".format(filename))
        if sig and signing == True:
            key = RSA.import_key(publickey)
            pkcs1_15.new(key).verify(new_hash, sig)
            print("Signature valid!")
    f = open(path, 'wb')
    print(path)
    f.write(memfile)
    f.close()
    c = conn_l.cursor
    # if upd:
        # c.execute('UPDATE files SET revision=revision+1 WHERE path=?', path)
        # c.execute('UPDATE files SET checksum=? WHERE path=?', (fhash, path))
        # c.execute('UPDATE files SET checksumlzma=? WHERE path=?', ("0", path))
        # c.execute('UPDATE files SET signature=? WHERE path=?', (sig, path))
    # else:
        # c.execute('INSERT INTO files VALUES (?,?,?,?,?)', (path, 0, fhash, "0", sig))
    conn_l.commit()
    conn_l.close()
    print("File download complete!")
    return 0


def argvparse():
    global prefix
    global keyfile
    global signing
    global hashing
    global url
    global nproc
    global cfg_write
    uhelp = """\
Usage: ofatomic -p . [-k (ofpublic.pem)] [-u (default server url)] [-n 4] [--disable-hashing] [--disable-signing]
Command line installer for Open Fortress.
  -p: Choose desired path for installation. Mandatory.
  -k: Specify public key file to verify signatures against. Default is the current OF public key (ofpublic.pem).
  -n: Amount of threads to be used - choose 1 to disable multithreading. Default is the number of threads in the system.
  -u: Specifies URL to download from. Specify the protocol (https:// or http://) as well. Default is the OF repository.
  -h: Displays this help message.
  --disable-hashing: Disables hash checking when downloading.
  --disable-signing: Disables signature checking when downloading.
  --cfg-overwrite: Overwrite existing .cfg files. Off by default."""
    if len(argv) == 1 or '-h' in argv:
        print(uhelp)
        exit()
    if '-p' in argv:
        prefix = Path(argv[argv.index('-p') + 1])
    else:
        print("no path!")
        exit()
    if '-k' in argv:
        keyfile = Path(argv[argv.index('-k') + 1])
    else:
        keyfile = mpath / Path("ofpublic.pem")
    if '-n' in argv:
        nproc = int(argv[argv.index('-n') + 1])
    if '--disable-hashing' in argv:
        hashing = False
    if '--disable-signing' in argv:
        signing = False
    if '--cfg-overwrite' in argv:
        cfg_write = True
    if '-u' in argv:
        url = argv[argv.index('-u') + 1]
        if url[-1:] != '/':
            url += '/'


def main():
    global keyfile
    global nproc
    global cfg_write
    global mpath
    argvparse()
    rpath = prefix / Path('launcher/remote/ofmanifest.db')
    lpath = prefix / Path('launcher/local/ofmanifest.db')
    if not (exists(rpath) and getsize(rpath) > 0):
        makedirs(rpath.parents[0], exist_ok=True)
        download_db(rpath.parents[0])
    rdb = open(rpath,'r')
    if not (lpath.exists() and lpath.stat().st_size > 0):
        makedirs(lpath.parents[0], exist_ok=True)
        nolocal = True
    else:
        nolocal = False
    ldb = open(lpath,"w")
    c = csv.DictReader(rdb)
    remotedict = [c for c in c]
    todl = []
    with open(keyfile, 'r') as w:
        keydata = w.read()
    if nolocal:
        todl = [(f["path"], f["comphash"], f["signature"], str(prefix), keydata,False) for f in remote]
    else:
        upd = False
        local = cl.execute("select path,checksum,signature from files").fetchall()
        for f in remote:
            if f in local:
                continue
            elif f[0] in local:
                upd = True
            if not cfg_write:
                if f[0] in local and ".cfg" in f[0]:
                    continue
            todl.append((f[0], f[1], f[2], str(prefix), keydata,conn_l,upd))
    x = []
    if nproc > 1:
        try:
            dpool = Pool(nproc)
            x = list(dpool.starmap(download_file_multi, todl))
        except ImportError:
            nproc = 1
    if nproc <= 1:
        x = list(starmap(download_file_multi,todl))
    conn.close()
    with open(mpath / Path("gameinfo.txt"), 'r') as gd:
        gd_l = open(prefix / Path("gameinfo.txt"), 'w')
        gd_l.write(gd.read())
        gd_l.close()
    print(x)
    if 1 in x:
        print("OF download timed out at some point, you should be able to redownload...")
    print("OF download completed!")
