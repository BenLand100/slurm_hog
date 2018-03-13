#!/usr/bin/env python3

import argparse
import sqlite3
import json
import time
import os, sys
import subprocess
import threading

db = None

def setup_database(args):
    global db
    db = sqlite3.connect(args.db)

def init(args):
    if os.path.exists(args.db):
        print('Database file already exists. First delete it to create a new one.')
        sys.exit(1)
    setup_database(args)
    c = db.cursor()
    c.execute('CREATE TABLE jobs (jobid INTEGER PRIMARY KEY AUTOINCREMENT, exec TEXT, cwd TEXT, stdout TEXT, stderr TEXT, env TEXT, status TEXT, heartbeat INTEGER);')
    c.execute('CREATE INDEX job_status ON jobs(status);')    
    c.execute('CREATE INDEX job_heartbeat ON jobs(heartbeat,status);')
    db.commit()
    
def submit(args):
    setup_database(args)
    cwd = os.getcwd()
    env = json.dumps(dict(**os.environ))
    c = db.cursor()
    c.execute("INSERT INTO jobs (exec,cwd,stdout,stderr,env,status,heartbeat) VALUES (?,?,?,?,?,'waiting',0);",(args.executable,cwd,args.stdout,args.stderr,env))
    db.commit()

def cancel(args):
    setup_database(args)
    c = db.cursor()
    c.execute('DELETE FROM jobs WHERE jobid = ?;',(args.jobid,))
    db.commit()

def hog_launch(semp,executable,cwd,stdout,stderr,env):
    try:
        os.chdir(cwd)
        fout=open(stdout if stdout else os.devnull,'w')
        ferr=open(stdout if stdout else os.devnull,'w')
        env=json.loads(env)
        subproc = subprocess.Popen([executable],stdout=fout,stderr=ferr,env=env,preexec_fn=os.setsid)
        def sub_wait(subproc):
            subproc.wait()
            semp.release()
        thread = threading.Thread(target=sub_wait,args=(subproc,))
        thread.start()
        return subproc
    except:
        semp.release()
        return None

def hog_alloc(jobs,semp):
    isolvl = db.isolation_level
    db.isolation_level = None
    c = db.cursor()
    c.execute('BEGIN EXCLUSIVE;');
    while semp.acquire(blocking=False):
        c.execute("SELECT jobid, exec, cwd, stdout, stderr, env FROM jobs WHERE status='waiting' LIMIT 1;")
        row = c.fetchone()
        if row is None:
            semp.release()
            break
        jobid,executable,cwd,stdout,stderr,env = row
        c.execute("UPDATE jobs SET status='running',heartbeat=? WHERE jobid = ?;",(time.time(),jobid))
        subproc = hog_launch(semp,executable,cwd,stdout,stderr,env)
        if subproc:
            print('allocated',jobid,executable)
            jobs[jobid] = subproc
        else:
            print('failed to allocate',jobid)
            c.execute("UPDATE jobs SET status='failed' WHERE jobid = ?;",(jobid,))
    c.execute('COMMIT;')
    db.isolation_level = isolvl

def hog_check(jobs):
    c = db.cursor()
    for jobid in jobs.keys():
        print('checking',jobid)
        subproc = jobs[jobid]
        c.execute('SELECT status FROM jobs WHERE jobid = ?;',(jobid,))
        status = c.fetchone()[0]
        if status == 'canceled':
            print('canceled',jobid)
            os.killpg(os.getpgid(subproc.pid), signal.SIGTERM) #does this release semp?
            del jobs[jobid]
            continue
        status = subproc.poll()
        if status is None:
            print('heartbeat',jobid)
            c.execute('UPDATE jobs SET heartbeat=? WHERE jobid = ?;',(time.time(),jobid))
        else:
            print('finished',jobid)
            c.execute("UPDATE jobs SET status='done',heartbeat=? WHERE jobid = ?;",(time.time(),jobid))
            del jobs[jobid]
    db.commit()
            
def hog(args):
    setup_database(args)
    semp = threading.Semaphore(value=args.simultaneous)
    start = time.time()
    args.time = args.time*60*60
    args.moratorium = args.moratorium*60*60
    jobs = {}
    try:
        while args.time-(time.time()-start) > 120: #quit with 2  minutes to spare
            hog_check(jobs)
            print(jobs)
            if args.time-(time.time()-start) >= args.moratorium and semp.acquire(timeout=10):
                semp.release()
                hog_alloc(jobs,semp)
                print(jobs)
            if len(jobs) == 0:
                break #no jobs
            if semp.acquire(blocking=False): #not fully allocated, wait a bit
                semp.release()
                time.sleep(10)
    except KeyboardInterrupt:
        print('hog ctrl-c\'d')
    if len(jobs) > 0: #reset any jobs 
        c = db.cursor()
        for jobid in jobs.keys():
            print('outoftime',jobid)
            c.execute("UPDATE jobs SET status='outoftime' WHERE jobid = ?;",(jobid,))
            subproc = jobs[jobid]
            os.killpg(os.getpgid(subproc.pid), signal.SIGTERM) #does this release semp?
        
def monitor_check():
    c = db.cursor()
    stale = time.time()
    c.execute("SELECT jobid FROM jobs WHERE status='running' AND heartbeat<?",(stale,))
    stalejobs = [row[0] for row in c.fetchall()]
    for jobid in stalejobs:
        c.execute("UPDATE jobs SET status='stale' WHERE jobid=?",(jobid,))

def monitor_launch(semp):
    print(__file__)
    subproc = subprocess.Popen(['./slurm_hog.py','hog','-s',str(args.simultaneous),'-t',str(args.time),'-m',str(args.moratorium)])
    def sub_wait(subproc,semp):
        subproc.wait()
        semp.release()
    thread = threading.Thread(target=sub_wait,args=(subproc,semp))
    thread.start()

def monitor(args):
    setup_database(args)
    semp = threading.Semaphore(value=args.batches)
    try:
        while True:
            monitor_check()
            while semp.acquire(timeout=10):
                monitor_launch(semp)
    except KeyboardInterrupt:
        print('monitor ctrl-c\'d')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='queues jobs to run in batches on a slurm queue')
    parser.add_argument('--db',metavar='file',default='jobs.sqlite',help='sqlite database to interact with')
    subparsers = parser.add_subparsers(title='subcommands',description='valid subcomands')
    
    init_parser = subparsers.add_parser('init',help='create a new jobs database')
    init_parser.set_defaults(func=init)
    
    submit_parser = subparsers.add_parser('submit',help='submit a job')
    submit_parser.set_defaults(func=submit)
    submit_parser.add_argument('executable',help='The executable to run')
    submit_parser.add_argument('-o','--stdout',metavar='file',help='file to save standard output to')
    submit_parser.add_argument('-e','--stderr',metavar='file',help='file to save standard error to')
    
    cancel_parser = subparsers.add_parser('cancel',help='cancel a job')
    cancel_parser.set_defaults(func=cancel)
    cancel_parser.add_argument('jobid',help='ID of a submitted job')
    
    hog_parser = subparsers.add_parser('hog',help='the subcommand for jobs submitted to the slurm backend')
    hog_parser.set_defaults(func=hog)
    hog_parser.add_argument('-s','--simultaneous',type=int,default=24,help='number of simultaneous submitted jobs per hog job')
    hog_parser.add_argument('-t','--time',type=int,default=72,help='max wall time of each hog job (hours)')
    hog_parser.add_argument('-m','--moratorium',default=12,type=int,help='minimum wall time remaining required to submit a job (hours)')

    monitor_parser = subparsers.add_parser('monitor',help='submit and monitor hog jobs on the slurm backend')
    monitor_parser.set_defaults(func=monitor)
    monitor_parser.add_argument('-b','--batches',type=int,default=1,help='number of hog jobs to run at once')
    monitor_parser.add_argument('-s','--simultaneous',type=int,default=24,help='number of simultaneous processes per hog job')
    monitor_parser.add_argument('-t','--time',type=int,default=72,help='max wall time of each hog job (hours)')
    monitor_parser.add_argument('-m','--moratorium',default=12,type=int,help='minimum wall time remaining required to submit a job (hours)')
    
    args = parser.parse_args()
    args.func(args)
    
    
    
