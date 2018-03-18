#!/usr/bin/env python3

import argparse
import apsw
import json
import time
import socket
import os, sys, signal
import subprocess
import threading

db = None

def setup_database(args,ignore_missing=False):
    global db
    if not ignore_missing and not os.path.exists(args.db):
        print('Please create a database file before using slurm_hog.py.')
        sys.exit(1)
    db = apsw.Connection(args.db)
    db.setbusytimeout(args.timeout*1000)
    c = db.cursor()
    c.execute('PRAGMA foreign_keys = ON;')

def sub_wait(subproc,semp):
    subproc.wait()
    semp.release()

def init(args):
    if os.path.exists(args.db):
        print('Database file already exists. First delete it to create a new one.')
        sys.exit(1)
    setup_database(args,ignore_missing=True)
    c = db.cursor()
    c.execute('CREATE TABLE jobs (jobid INTEGER PRIMARY KEY AUTOINCREMENT, exec TEXT, cwd TEXT, stdout TEXT, stderr TEXT, env TEXT, status TEXT, heartbeat INTEGER);')
    c.execute('CREATE INDEX job_status ON jobs(status);')    
    c.execute('CREATE INDEX job_heartbeat ON jobs(heartbeat,status);')
    c.execute('CREATE TABLE hogs (hogid INTEGER PRIMARY KEY AUTOINCREMENT, pid INTEGER, hostname TEXT, submittime INTEGER, starttime INTEGER, status TEXT, heartbeat INTEGER);')
    c.execute('CREATE INDEX hog_status ON hogs(status);')
    c.execute('CREATE INDEX hog_heartbeat ON hogs(heartbeat,status);')
    c.execute('CREATE TABLE alloc (jobid INTEGER, hogid INTEGER, FOREIGN KEY(jobid) REFERENCES jobs(jobid) ON DELETE CASCADE, FOREIGN KEY(hogid) REFERENCES hogs(hogid) ON DELETE CASCADE, PRIMARY KEY (jobid,hogid));')
    
def submit(args):
    setup_database(args)
    cwd = os.getcwd()
    env = json.dumps(dict(**os.environ))
    c = db.cursor()
    c.execute("BEGIN;")
    c.execute("INSERT INTO jobs (exec,cwd,stdout,stderr,env,status,heartbeat) VALUES (?,?,?,?,?,'waiting',0);",(args.executable,cwd,args.stdout,args.stderr,env))
    c.execute("SELECT last_insert_rowid() FROM jobs;")
    row = c.fetchone()
    c.execute("COMMIT;")
    print(row[0])

def cancel(args):
    setup_database(args)
    c = db.cursor()
    c.execute("UPDATE jobs SET status='canceled' WHERE jobid = ?;",(args.jobid,))

def check(args):
    setup_database(args)
    c = db.cursor()
    c.execute("SELECT status FROM jobs WHERE jobid=?;",(args.jobid,))
    row = c.fetchone()
    if row is None:
        print('job',args.jobid,'not in database')
    else:
        print(row[0])

def cleanup(args):
    setup_database(args)
    c = db.cursor()
    c.execute("DELETE FROM jobs WHERE status!='waiting' AND status!='running';")

def show_hogs(args):
    c = db.cursor()
    c.execute("SELECT hogid,status,pid FROM hogs;")
    for hogid,status,pid in c.fetchall():
        print(hogid,status,pid)

def show_jobs(args):
    c = db.cursor()
    if args.status is None:
        c.execute("SELECT jobid,status FROM jobs;")
        for jobid,status in c.fetchall():
            print(jobid,status)
    else:
        print(args.status)
        for status in args.status:
            c.execute("SELECT jobid,status FROM jobs WHERE status=?",(status,))
            for jobid,status in c.fetchall():
                print(jobid,status)

def show(args):
    setup_database(args)
    if args.hogs:
        show_hogs(args)
    else:
        show_jobs(args)
        
def hog_launch(semp,jobid,executable,cwd,stdout,stderr,env):
    try:
        os.chdir(cwd)
        fout=open(stdout if stdout else os.devnull,'w')
        ferr=open(stdout if stdout else os.devnull,'w')
        env=json.loads(env)
        env['JOBID'] = str(jobid)
        subproc = subprocess.Popen([executable],stdout=fout,stderr=ferr,env=env,preexec_fn=os.setsid)
        thread = threading.Thread(target=sub_wait,args=(subproc,semp),daemon=True)
        thread.start()
        return subproc
    except:
        semp.release()
        return None

def hog_alloc(hogid,jobs,semp):
    c = db.cursor()
    print('hog allocating jobs')
    while semp.acquire(blocking=False):
        c.execute("BEGIN EXCLUSIVE;")
        c.execute("SELECT jobid, exec, cwd, stdout, stderr, env FROM jobs WHERE status='waiting' LIMIT 1;")
        row = c.fetchone()
        if row is None:
            semp.release()
            c.execute("COMMIT;")
            break
        jobid,executable,cwd,stdout,stderr,env = row
        c.execute("UPDATE jobs SET status='running',heartbeat=? WHERE jobid = ?;",(time.time(),jobid))
        c.execute("INSERT INTO alloc (jobid,hogid) VALUES (?,?)",(jobid,hogid))
        c.execute("COMMIT;")
        subproc = hog_launch(semp,jobid,executable,cwd,stdout,stderr,env)
        if subproc:
            print('allocated',jobid,executable)
            jobs[jobid] = subproc
        else:
            print('failed to allocate',jobid)
            c.execute("UPDATE jobs SET status='failed' WHERE jobid = ?;",(jobid,))

def hog_check(jobs):
    c = db.cursor()
    jobids = list(jobs.keys())
    print('hog running checks')
    for jobid in jobids:
        print('checking',jobid)
        subproc = jobs[jobid]
        c.execute('SELECT status FROM jobs WHERE jobid = ?;',(jobid,))
        status = c.fetchone()[0]
        if status == 'canceled':
            print('canceled',jobid)
            try:
                os.killpg(os.getpgid(subproc.pid), signal.SIGTERM) #does this release semp?
            except:
                print('could not kill',jobid)
            del jobs[jobid]
            continue
        status = subproc.poll()
        if status is None:
            print('heartbeat',jobid)
            c.execute("UPDATE jobs SET status='running',heartbeat=? WHERE jobid = ?;",(time.time(),jobid))
        else:
            print('finished',jobid)
            c.execute("UPDATE jobs SET status='done',heartbeat=? WHERE jobid = ?;",(time.time(),jobid))
            del jobs[jobid]

def hog(args):
    print('test')
    setup_database(args)
    c = db.cursor()
    semp = threading.Semaphore(value=args.simultaneous)
    hostname = socket.gethostname()
    start = time.time()
    c.execute("UPDATE hogs SET hostname=?,starttime=?,status='running',heartbeat=? WHERE hogid=?;",(hostname,start,start,args.hogid))
    args.time = args.time*60*60
    args.moratorium = args.moratorium*60*60
    jobs = {}
    try:
        print('a wild hog has appeared on',hostname)
        while args.time-(time.time()-start) > 120: #quit with 2  minutes to spare
            loopstart = time.time()
            c.execute("UPDATE hogs SET heartbeat=?,status='running' WHERE hogid=?;",(time.time(),args.hogid))
            hog_check(jobs)
            print('allocated jobs: ',len(jobs))
            if args.time-(time.time()-start) >= args.moratorium and semp.acquire(timeout=60):
                semp.release()
                hog_alloc(args.hogid,jobs,semp)
                print('allocated jobs: ',len(jobs))
            if len(jobs) == 0:
                break #no jobs
            looptime = time.time() - loopstart
            if looptime < 60: #heartbeat no faster than each minute
                time.sleep(60-looptime)
    except KeyboardInterrupt:
        print('hog ctrl-c\'d')
    c.execute("UPDATE hogs SET heartbeat=?,status='done' WHERE hogid=?;",(time.time(),args.hogid))
    if len(jobs) > 0: #reset any jobs 
        for jobid in jobs.keys():
            print('outoftime',jobid)
            c.execute("UPDATE jobs SET status='outoftime' WHERE jobid = ?;",(jobid,))
            subproc = jobs[jobid]
            try:
                os.killpg(os.getpgid(subproc.pid), signal.SIGTERM) #does this release semp?
            except:
                print('could not kill',jobid)
        
def monitor_check(external_pids,semp):
    c = db.cursor()
    stale = time.time() - 10*60 #10 minutes
    c.execute("SELECT jobid FROM jobs WHERE status='running' AND heartbeat<?",(stale,))
    stalejobs = [row[0] for row in c.fetchall()]
    for jobid in stalejobs:
        c.execute("UPDATE jobs SET status='stale' WHERE jobid=?",(jobid,))
    c.execute("SELECT hogid,pid FROM hogs WHERE status='running' AND heartbeat<?",(stale,))
    stalehogs = [(hogid,pid) for hogid,pid in c.fetchall()]
    for hogid,pid in stalehogs:
        c.execute("UPDATE hogs SET status='stale' WHERE hogid=?",(hogid,))
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
            os.killpg(os.getpgid(pid), signal.SIGTERM) #slurm likes abuse
        except:
            pass
        if pid in external_pids:
            semp.release()
            

def monitor_launch(args,semp):
    c = db.cursor()
    c.execute('BEGIN;')
    c.execute("INSERT INTO hogs (submittime, status) VALUES (?,'waiting');",(time.time(),))
    c.execute("SELECT last_insert_rowid() FROM hogs;")
    hogid = c.fetchone()[0]
    c.execute("COMMIT;")
    cmd = args.command_prefix.split()+[__file__,'--db',args.db,'--timeout',str(args.timeout),'hog',str(hogid),'-s',str(args.simultaneous),'-t',str(args.time),'-m',str(args.moratorium)]
    subproc = subprocess.Popen(cmd,preexec_fn=os.setsid)
    c.execute("UPDATE hogs SET pid=? WHERE hogid=?",(subproc.pid,hogid))
    thread = threading.Thread(target=sub_wait,args=(subproc,semp),daemon=True)
    thread.start()

def monitor(args):
    setup_database(args)
    semp = threading.Semaphore(value=args.batches)
    c = db.cursor()
    c.execute("SELECT pid FROM hogs WHERE status='running' OR status='waiting'") #jobs removed from slurm externally could get stuck waiting
    external_pids = [row[0] for row in c.fetchall()]
    for pid in external_pids:
        semp.acquire()
    try:
        while True:
            monitor_check(external_pids,semp)
            while semp.acquire(timeout=10):
                monitor_launch(args,semp)
                time.sleep(1)
    except KeyboardInterrupt:
        print('monitor ctrl-c\'d')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='queues jobs to run in batches on a slurm queue')
    parser.add_argument('--db',metavar='FILE',default='jobs.sqlite',help='sqlite database to interact with')
    parser.add_argument('--timeout',metavar='SECONDS',type=int,default=300,help='sqlite database timeout')
    subparsers = parser.add_subparsers(dest='subcommand',metavar='subcommand',help='use `[subcommand] --help` for additional help')
    subparsers.required = True
    
    init_parser = subparsers.add_parser('init',help='create a new jobs database')
    init_parser.set_defaults(func=init)

    cleanup_parser = subparsers.add_parser('cleanup',help='remove all non-waiting and non-running jobs from database')
    cleanup_parser.set_defaults(func=cleanup)

    show_parser = subparsers.add_parser('show',help='show jobs in the database')
    show_parser.set_defaults(func=show)
    show_parser.add_argument('-s','--status',action='append',default=None,help='status to show')
    show_parser.add_argument('-H','--hogs',action='store_true',help='show hogs instead of jobs')
    
    submit_parser = subparsers.add_parser('submit',help='submit a job')
    submit_parser.set_defaults(func=submit)
    submit_parser.add_argument('executable',help='The executable to run')
    submit_parser.add_argument('-o','--stdout',metavar='file',help='file to save standard output to')
    submit_parser.add_argument('-e','--stderr',metavar='file',help='file to save standard error to')
    
    cancel_parser = subparsers.add_parser('cancel',help='cancel a job')
    cancel_parser.set_defaults(func=cancel)
    cancel_parser.add_argument('jobid',help='ID of a submitted job')

    check_parser = subparsers.add_parser('check',help='check job status')
    check_parser.set_defaults(func=check)
    check_parser.add_argument('jobid',help='ID of a submitted job')

    hog_parser = subparsers.add_parser('hog',help='the subcommand for jobs submitted to the backend')
    hog_parser.set_defaults(func=hog)
    hog_parser.add_argument('hogid',type=int,help='index in the database representing this hog job')
    hog_parser.add_argument('-s','--simultaneous',type=int,default=24,help='number of simultaneous submitted jobs per hog job')
    hog_parser.add_argument('-t','--time',type=int,default=72,metavar='HOURS',help='max wall time of each hog job')
    hog_parser.add_argument('-m','--moratorium',default=12,type=int,help='minimum wall time remaining required to submit a job (hours)')

    monitor_parser = subparsers.add_parser('monitor',help='submit and monitor hog jobs on the slurm backend')
    monitor_parser.set_defaults(func=monitor)
    monitor_parser.add_argument('-c','--command-prefix',default='nohup',help='command prefix (srun ...) to launch hog jobs on compute nodes') #not ideal, could compute -t
    monitor_parser.add_argument('-b','--batches',type=int,default=1,help='number of hog jobs to run at once')
    monitor_parser.add_argument('-s','--simultaneous',type=int,default=20,help='number of simultaneous processes per hog job')
    monitor_parser.add_argument('-t','--time',type=int,default=72,metavar='HOURS',help='max wall time of each hog job')
    monitor_parser.add_argument('-m','--moratorium',default=12,type=int,metavar='HOURS',help='minimum wall time remaining required to submit a job')
    
    args = parser.parse_args()
    args.func(args) 
