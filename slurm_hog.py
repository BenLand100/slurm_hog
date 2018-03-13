#!/usr/bin/env python

import argparse
import sqlite3
import json
import time
import os, sys
import subprocess

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

def hog_alloc(jobs,max_alloc):
    isolvl = db.isolation_level
    db.isolation_level = None
    c = db.cursor()
    c.execute('BEGIN EXCLUSIVE;');
    c.execute("SELECT jobid, exec, cwd, stdout, stderr, env FROM jobs WHERE status='waiting' LIMIT ?;",(max_alloc,))
    for jobid,executable,cwd,stdout,stderr,env in c.fetchall():
        try:
            c.execute("UPDATE jobs SET status='running',heartbeat=? WHERE jobid = ?;",(time.time(),jobid))
            print('allocating',jobid,executable)
            os.chdir(cwd)
            fout=open(stdout if stdout else os.devnull,'w')
            ferr=open(stdout if stdout else os.devnull,'w')
            env=json.loads(env)
            subproc = subprocess.Popen([executable],stdout=fout,stderr=ferr,env=env,preexec_fn=os.setsid)
            jobs[jobid] = subproc
        except:
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
            os.killpg(os.getpgid(subproc.pid), signal.SIGTERM)
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
            
def monitor(args):
    setup_database(args)
    jobs = {}
    while True:
        hog_check(jobs)
        print jobs
        hog_alloc(jobs,10-len(jobs))
        print jobs
        time.sleep(10)
        
    

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
    
    monitor_parser = subparsers.add_parser('monitor',help='submit and monitor hog jobs to the slurm backend')
    monitor_parser.set_defaults(func=monitor)
    monitor_parser.add_argument('-b','--batches',type=int,default=1,help='number of hog jobs to run at once')
    monitor_parser.add_argument('-s','--simultaneous',type=int,default=24,help='number of simultaneous submitted jobs per hog job')
    monitor_parser.add_argument('-t','--time',type=int,default=72,help='max wall time of each hog job (hours)')
    monitor_parser.add_argument('-m','--moratorium',default=12,type=int,help='minimum wall time remaining required to submit a job (hours)')
    
    args = parser.parse_args()
    args.func(args)
    
    
    
