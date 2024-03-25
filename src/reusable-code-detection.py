import os, sys, re, shutil, subprocess, re, platform, json, pickle, timeit, multiprocessing, traceback, argparse, threading, math, random, logging
from sys import stderr
from tqdm import tqdm
# from numba import jit, cuda
from timeit import default_timer as timer
import pandas as pd
import concurrent.futures as cf
from git import Repo
from glob import glob
from time import time, sleep

# Run shell command from a string
sys.path.append(os.getcwd())
sys.path.append("..")
from config import config_global
from utils import Git_repo
import numpy as np
from functools import partial
from queue import Queue as ThreadQueue
from multiprocessing import Queue as ProcessQueue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from psutil import virtual_memory, swap_memory, disk_usage
#from pydriller import Git
#from pydriller import Repository


'''
producer is to read from git repo related to I/O, so multi-threading is applied
'''
class Producer:

    def __init__(self, projects):

        self._config_logger()

        self.queue = ThreadQueue()
        # self.project_commit_status_all_df_path = os.path.join(config_global.LOG_PATH, 'project_commit_process_status_all_df.csv')
        # self.project_commit_status_all_df_lock = threading.Lock()
        # self.project_commit_status_all_df = pd.DataFrame()
        # self.project_commit_status_list = []
        self.interval = 10
        self.projects = projects 
        for project in projects:
            clone_result_path = os.path.join(config_global.CLONE_RESULT_PATH, project)
            if not os.path.exists(clone_result_path): # check if project clone_result path exists
                os.makedirs(clone_result_path, exist_ok=True) # cmd_mkdir_clone_result = 'mkdir -p %s/%s' % (config_global.CLONE_RESULT_PATH, project)

        self.max_workers = min(len(self.projects) + 1, int(os.cpu_count() * 3 / 4)) # [[todo][2023-07-18] we only use N processes if there are N projects (N < 50), if we later use more than 50 projects, we can use a max of N = 50 processes]
        
        self.project_commit_status_list_file = os.path.join(config_global.LOG_PATH, f'project_commit_status_list_{config_global.SERVER_NAME}.txt')


    def _config_logger(self):
        # configure logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        # Create a file handler
        log_dir = os.path.join(config_global.LOG_PATH, config_global.SERVER_NAME)
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f'{Producer.__name__}_running.log')
        if os.path.exists(log_file_path):
            os.remove(log_file_path)
        
        handler = logging.FileHandler(log_file_path)
        handler.setLevel(logging.INFO)
        
        # Create a logging format
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)
    

    def gitclone_projects(self, projects):
        for project in projects:
            print("project to clone: ", project)
            repo_path = os.path.join(config_global.REPO_PATH, f'nicad_workdir_{project}', project)
            Git_repo.gitclone_repo(project, repo_path)

            # create clone result path
            clone_result_path = os.path.join(config_global.CLONE_RESULT_PATH, project)
            os.makedirs(clone_result_path, exist_ok=True) # cmd_mkdir_clone_result = 'mkdir -p %s/%s' % (config_global.CLONE_RESULT_PATH, project)


    def git_checkout_commits(self, project, programming_lang, callback=None): # should not get_checkout_commit, since each commit will affact the working env of next commit
        # load the status file into memory for one project, create df with column name, empty df.
        nicad_workdir = os.path.join(config_global.REPO_PATH, f'nicad_workdir_{project}')
        project_repo_path = os.path.join(nicad_workdir, project)
        commits_to_run = self.get_commits_to_run(project, programming_lang)# [:20] # for commit_id in tqdm(project_commit_status_df['commit_id']):
        #self.logger.info(f'len commits_to_run:  - {len(commits_to_run)} ')

        # clean up the checkout working directory
        
        cmd_clean_nicad_workdir = 'rm -rf %s_*'% project_repo_path
        child = subprocess.Popen(cmd_clean_nicad_workdir, shell=True)
        child.poll()
        #self.logger.info(f'clean up the checkout working directory: {cmd_clean_nicad_workdir} ')
        # assert to make sure the folders are removed totally

        # accum_time = 0
        project_commit_status_list = []
        for commit_id in tqdm(commits_to_run, desc=f"Checkout commits for {project}"): # [todo] time from oldest to newest, if checkout fails, skip it and record it as errored instead of ready in status
            # [todo] curr_time = time() => time1
            #self.logger.info(f'\t working on ({project}-{commit_id}) ... ')
            cmd_git_checkout_commit = ['git', 'checkout', '-f', commit_id]
            execution_checkout = subprocess.run(cmd_git_checkout_commit, cwd=project_repo_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Git_repo.git_checkout_commit(project, commit_id)
            
            if execution_checkout.returncode == 0:
                #self.logger.info(f'\t\t check out ({project}-{commit_id}) successfully')
                project_commit_dir = os.path.join(nicad_workdir, f'{project}_{commit_id}')
                cmd_rsync = ['rsync', '-arp', project_repo_path, project_commit_dir] # note that cp is slow, use rsync to copy (https://www.zylk.net/en/web-2-0/blog/-/blogs/how-to-copy-files-in-linux-faster-and-safer-than-cp)
                execution_rsync = subprocess.run(cmd_rsync, cwd=nicad_workdir) # Git_repo.git_checkout_commit(project, commit_id)
                if execution_rsync.returncode == 0:
                    project_commit_status = {'project': project, 'commit_id': commit_id, 'status': 'ready', 'size': 0, 'lang': programming_lang, 'timestamp':time()}
                    # ignore using queue
                    self.queue.put(project_commit_status) # project_commit_status_df.loc[(project_commit_status_df['project'] == project) & (project_commit_status_df['commit_id'] == commit_id), 'status'] = 'ready'
                    # [todo] every 10mins (or other time), add lines to df in memory, end_time = time(), duration in minutes , accum_time += (time2 - time1), after writing, reset accum_time, overrite the original file
                    # [todo] add a logic to handle checkout interface
                    # sleep(1) # must sleep for a while, otherwise the checkout is too fast and exit because of disk issue
                    
                    # [todo] we need to make sure rsync is finished before we set status
                    # [todo] du -s project_commit_dir, to calculate the size, reg get the number for size, [0-9]+, make sure the command runs successfullfy
                    '''
                    cmd_dusize = ['du', '-s', project_commit_dir] 
                    #self.logger.info(f'\t\t rsync successfully! now get the size of {project}_{commit_id}_dir using: {cmd_dusize} ')
                    execution_dusize = subprocess.run(cmd_dusize, cwd=nicad_workdir, capture_output=True, text=True) # Git_repo.git_checkout_commit(project, commit_id)
                    if execution_dusize.returncode == 0:
                        size = int(execution_dusize.stdout.split('\t')[0])
                        project_commit_status = {'project': project, 'commit_id': commit_id, 'status': 'ready', 'size': size, 'lang': programming_lang, 'timestamp':time()} #[todo] df.append() <= [] 
                        #self.logger.info(f'\t\t dusize successfully! the size of {project}_{commit_id}_dir is: {size / 1048576} ')
                    else:
                        project_commit_status = {'project': project, 'commit_id': commit_id, 'status': 'ready', 'size': sys.maxsize, 'lang': programming_lang, 'timestamp':time()} #[todo] df.append() <= [] 
                        #self.logger.error(f'\t\t dusize ({project}-{commit_id}) failed - {execution_dusize.stderr}')
                        sys.exit(-1)
                    '''
                else:
                    #self.logger.error(f'\t\t rsync out ({project}-{commit_id}) failed - {execution_rsync.stderr}')
                    sys.exit(-1)
                
                # project_commit_status_list.append(project_commit_status)
                # https://stackoverflow.com/questions/27929472/improve-row-append-performance-on-pandas-dataframes
                # https://stackoverflow.com/questions/57000903/what-is-the-fastest-and-most-efficient-way-to-append-rows-to-a-dataframe/57001947#57001947
                
            # [todo] add elif checkout commit not exist, then continue instead of sys.exit, and record the project-commit_id as errored
            else:
                print(f'\t\t checkout ({project}-{commit_id}) failed - {execution_checkout.stderr}')
                self.logger.error(f'\t\t checkout ({project}-{commit_id}) failed - {execution_checkout.stderr}')
                sys.exit(-1)

            # sleep(3)
            # if swap_memory().percent > 80 or virtual_memory().percent > 80: #or disk_usage(path).percent > 80:
                #print("I need to sleep for a while, because the server is too busy! Zzzzzzz")
                #sleep(300) # sleep for 5 min
        
    @staticmethod
    def get_commits_to_run(project, programming_lang='c'):
        # only run commits that have changes, i.e., commit_id is in commit_modifications_dict
        commit_log_df = Git_repo.get_commit_log_df(project)
        
        # print("shape commit_log_df: ", commit_log_df.shape)
        '''
        commit_modifications_dict = Git_repo.get_commits_with_modifications(project, programming_lang)
        print("commit_modifications_dict: ", len(commit_modifications_dict))
        commit_modifications_dict_nonempty = {commit_id: modifications for commit_id, modifications in commit_modifications_dict.items() if modifications}
        commits_modified = commit_modifications_dict_nonempty.keys()
        
        # print("commits_modified: ", commits_modified)
        indices_commits_modified = commit_log_df[commit_log_df['commit_id'].isin(commits_modified)].index
        indices_commits_previous = indices_commits_modified + 1
        indices_commits_previous_valid = indices_commits_previous[indices_commits_previous.isin(commit_log_df.index)]
    
        indices_commits_to_run = np.concatenate([indices_commits_modified, indices_commits_previous_valid])
        indices_commits_to_run = np.unique(indices_commits_to_run)
        
        commit_ids_with_modifications = commit_log_df.loc[indices_commits_to_run]['commit_id']
        '''

        # resume from the clone detection breakpoint
        clone_result_dir = os.path.join(config_global.CLONE_RESULT_PATH_DATADIR, project)
        os.makedirs(clone_result_dir, exist_ok=True) # cmd_mkdir_clone_result = 'mkdir -p %s/%s' % (config_global.CLONE_RESULT_PATH, project)
        clone_result_extracted = [x.split(".")[0] for _, _, files in os.walk(clone_result_dir) for x in files] # remove the posix 
        commits_to_run = list(set(commit_log_df['commit_id'].tolist()) - set(clone_result_extracted))
        print(len(commit_log_df['commit_id'].tolist()), len(clone_result_extracted))
        # commits_to_run = list(set(commit_ids_with_modifications) - set(clone_result_extracted)) # commits_to_run = list(set(commit_log_df['commit_id']) - set(clone_result_extracted) - set()) 
        #commits_to_run = list(set(commit_log_df['commit_id'].tolist()) - set(clone_result_extracted))
        return commits_to_run
    

    # can ignore, replaced by save file for project separately
    def save_periodically(self, interval=60):
        os.makedirs(config_global.LOG_PATH, exist_ok=True)

        if os.path.exists(self.project_commit_status_list_file):
            os.remove(self.project_commit_status_list_file)
        
        self.logger.info(self.project_commit_status_list_file)
        while True:
            sleep(interval)  # Wait for 10 minutes
            with open(self.project_commit_status_list_file, 'a') as fp: # do not switch the position between withopen and whiletrue, otherwise the consumer can not listen to file update and react quickly
                self.logger.info(f"start writing project-commit status to {self.project_commit_status_list_file}")
                items_to_save = []
                while not self.queue.empty():
                    item = self.queue.get()
                    items_to_save.append(item)
                    self.logger.info(f"    getting item {item} from queue")

                self.logger.info(f"items_to_save: {items_to_save}")
                for item in items_to_save: # if the item is the sentinel, we're done
                    if item is None:
                        self.logger.info("reached None project")
                        project_commit_status_end = {'project': None, 'commit_id': None, 'status': None, 'size': 0, 'lang': None, 'timestamp':time()}
                        fp.write(json.dumps(project_commit_status_end) + '\n')
                        return
                    fp.write(json.dumps(item) + '\n')

                self.logger.info("finish save queue to file")


    def run(self):
        self.logger.info(f'start gitclone projects: {self.projects}')
        self.gitclone_projects(self.projects)
        
        executor = cf.ThreadPoolExecutor(max_workers=self.max_workers)
        worker_futures = []
        for project in self.projects:
            programming_lang = Git_repo.get_programming_language(project)
            worker_futures.append(executor.submit(self.git_checkout_commits, project, programming_lang))
            self.logger.info(f'project - {project} in language {programming_lang} is submitted')
        
        # future.add_done_callback(self.save_periodically) #for future in cf.as_completed(futures)
        save_future = executor.submit(self.save_periodically, 10)

        try:
            # Wait for all worker futures to complete
            for future in cf.as_completed(worker_futures):
                pass
            # self.queue.join()
        except Exception as e:
            print("exception: ", e)
        finally:
            self.queue.put(None)
            # Wait for the save future to complete
        
        save_future.result()
        
        # Ensure all tasks are completed before shutting down the executor
        executor.shutdown(wait=True)
        # Add sentinel to the queue to signal the end of the tasks
        

        #project_commit_status_end = {'project': None, 'commit_id': None, 'status': None, 'size': 0, 'lang': None, 'timestamp':time()}
        #with open(self.project_commit_status_list_file, 'a') as fp:
            #fp.write(json.dumps(project_commit_status_end) + '\n')
            

        
class Consumer:
    
    def __init__(self):
        self._config_logger()
        self.project_commit_status_all_df_path = os.path.join(config_global.LOG_PATH, 'project_commit_process_status_all_df.csv')
        self.num_workers = int(os.cpu_count() / 2)
        self.completed_futures_result = []
        self.num_processes = 0
        self.projects = list(config_global.SUBJECT_SYSTEMS.keys()) # (list(config_global.SUBJECT_SYSTEMS1.keys()) if config_global.SERVER_NAME=='gpu-01'  else list(config_global.SUBJECT_SYSTEMS2.keys()))
        self.project_commit_status_list_file = os.path.join(config_global.LOG_PATH, f'project_commit_status_list_{config_global.SERVER_NAME}.txt')


    def _config_logger(self):
        # configure logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        # Create a file handler
        log_dir = os.path.join(config_global.LOG_PATH, config_global.SERVER_NAME)
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f'{Consumer.__name__}_running.log')
        if os.path.exists(log_file_path):
            os.remove(log_file_path)
        
        handler = logging.FileHandler(log_file_path)
        handler.setLevel(logging.INFO)
        
        # Create a logging format
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)


    def get_project_commit_status_ready_df(self):
        project_commit_status_all_df = pd.read_csv(self.project_commit_status_all_df_path)
        project_commit_ready_all_df = project_commit_status_all_df.loc[project_commit_status_all_df['status'] == 'ready']
        return project_commit_ready_all_df


    def detect_clones_on_commit(self, project, commit_id, programming_lang):
        # remove the dest file if exists
        clone_file_dest_path = os.path.join(config_global.CLONE_RESULT_PATH, project, f'{commit_id}.xml')
        if os.path.exists(clone_file_dest_path):
            return project, commit_id, 'done', 0 # os.remove(clone_file_dest_path) #shellCommand('rm -rf %s' % dest_path)

        nicad_workdir = os.path.join(config_global.REPO_PATH, 'nicad_workdir_%s' % project)
        #print("nicad_workdir: ", nicad_workdir)
        project_commit_dir = os.path.normpath(os.path.join(nicad_workdir, f'{project}_{commit_id}'))
        #print("project_commit_dir: ", project_commit_dir)
        if not os.path.exists(project_commit_dir):
            print(f"{project_commit_dir} not found")
            sys.exit(-1)
    
        #if os.path.exists(clone_file_dest_path):
            #os.remove(clone_file_dest_path) #shellCommand('rm -rf %s' % dest_path)
        
        # perform clone detection by NiCad
        # print("project_commit_dir: ", project_commit_dir)
        # os.chdir(project_commit_dir) # to make nicad run under this directory
        cmd_detect_clones = ['nicad6', 'functions', programming_lang, project_commit_dir]
        execution_detect_clones = subprocess.run(cmd_detect_clones, cwd=project_commit_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Git_repo.git_checkout_commit(project, commit_id)
        if execution_detect_clones.returncode == 0:
            nicad_workdir = os.path.join(config_global.REPO_PATH, 'nicad_workdir_%s' % project)
            nicad_clone_file_path = os.path.join(f'{project_commit_dir}_functions-blind-clones', f'{project}_{commit_id}_functions-blind-clones-0.30-classes-withsource.xml') # check
            
            # print("clone_file_src_path: ", nicad_clone_file_path)
            if os.path.exists(nicad_clone_file_path):
                # move the results to the result folder
                try:
                    # move the results to the result folder
                    shutil.move(nicad_clone_file_path, clone_file_dest_path)
                    shutil.rmtree(project_commit_dir)
                except Exception as e:
                    print(f"{nicad_clone_file_path} exists, Move clone-detection result operation failed: {e}")
                    sys.exit(-1)
            
            # cleanup the nicad working directory or use nicad cleanall, be care of the wildcard
            nicad_workdir_output_pattern = f'{project}_{commit_id}_functions*'
            nicad_workdir_output_pattern_path = os.path.join(nicad_workdir, nicad_workdir_output_pattern)
            cmd_clean_nicad_workdir = ['rm', '-rf', f'{nicad_workdir}/{project}_{commit_id}_functions*']
            # child = subprocess.Popen(cmd_clean_nicad_workdir, shell=True)
            # child.poll()
            globbed_paths = glob(nicad_workdir_output_pattern_path)
            for path in globbed_paths:
                os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
    
            return project, commit_id, 'done', time()
            
            # clean memory
            # shellCommand(cmd_clear_cache) cmd_clear_cache = 'sudo sysctl -w vm.drop_caches=3' # no privileges on gpu1
        else:
            print(f"nicad detect clones failed for {project}-{commit_id}")
            sys.exit(-1)


    def collect_process_status(self):
        # print("*************** len futures: ", len(self.completed_futures_result))
        
        while True: 
            len_completed_futures_result = len(self.completed_futures_result)
            # print("okay ", len_completed_futures_result)
            # print("*************** len futures: ", len(self.futures))
            sleep(600)  # Save periodically every 10 min
            
            # print(self.completed_futures_result)
            for project_commit_process_status in self.completed_futures_result:
                (project, commit_id, status, timestamp) = project_commit_process_status
                self.project_commit_ready_all_df.loc[(self.project_commit_ready_all_df['project'] == project) & (self.project_commit_ready_all_df['commit_id'] == commit_id), 'status'] = status
                self.project_commit_ready_all_df.loc[(self.project_commit_ready_all_df['project'] == project) & (self.project_commit_ready_all_df['commit_id'] == commit_id), 'timestamp'] = timestamp   
            
            self.project_commit_ready_all_df.to_csv(os.path.join(config_global.LOG_PATH, 'clone_detection_done.csv'), mode='w', index=False)

            if len_completed_futures_result == self.num_processes:
                break


    def _estimate_num_processes(self, projects):
        total_project_commits_lines = 0
        for project in projects:
            commit_log_df = Git_repo.get_commit_log_df(project)
            total_project_commits_lines += commit_log_df.shape[0]
            #result = subprocess.run(['wc', '-l', project_log_file], stdout=subprocess.PIPE)
            #if result:
                #print("result: ", result)
                #num_lines =  int(result.stdout.split()[0])
                #total_project_commits_lines += num_lines
        return total_project_commits_lines
    

    def run(self):
        # [todo] separate NiCad on server2
        # [todo] step1: num_workders is on project-commit-id and is project unrelated
        # [todo] step2: before multi-processing of NiCad, merge each project status files into one, sort by size ascending and distribute the 50 smallest project-commit to 50 processes, another batch of 50 continue distribute to the 50 processes 
        # [todo] step3: num % 50 all to the same queue and feed into the same proceses, for-loop the queue, 
        # [todo] step4: we will wait nicad to finish the current merged file
        # [todo] step5: make checkout every 10min to write a new file
        # [todo] step6: then continue from 4, nicad read the newest status file
        # [todo] restart step 2-3-4
        # [todo] step7: update nicad status files to log, (merged - nicad_status_file) is the tasks to run
        # [todo] step8: check the error, (mem, disk, swap)

        # Create a new file change handler and start the observer
        task_queue = ProcessQueue()
        
        event_handler = FileChangeHandler(self.project_commit_status_list_file, task_queue)
        observer = Observer()
        observer.schedule(event_handler, path=self.project_commit_status_list_file, recursive=False)
        observer.start()

        executor = cf.ProcessPoolExecutor(max_workers=self.num_workers)
        futures = []
        
        # Create a progress bar
        pbar = tqdm(total=self._estimate_num_processes(self.projects))
        while True: # not task_queue.empty():
            project, commit_id, status, size, lang, timestamp = task_queue.get()  # get task from queue
            #print("project-commit_id: ", project, commit_id, status, size, lang, timestamp )
            if project is None:  # We use (None, None) as a signal of the end of tasks
                # print("ayayayya  there is noneeeeeeeeeeeeeeeeeeeeeee")
                break

            future = executor.submit(self.detect_clones_on_commit, project, commit_id, lang)
            future.add_done_callback(lambda x: pbar.update())  # Increment the progress bar when a task is done
            futures.append(future)
            # print("================ len futures: ", len(futures))

            # If the future is running, then it has definitely started
            '''
            if future.running():
                print(f"{project}_{commit_id} task has started")
            else:
                print(f"{project}_{commit_id} task has not started yet")
            '''
            

        for future in cf.as_completed(futures):
            # print(future.result)
            self.completed_futures_result.append(future.result())

        #observer.stop()
        #observer.join()
        observer.stop()  # Stop observing
        executor.shutdown(wait=True)
        '''
        self.project_commit_ready_all_df = self.get_project_commit_status_ready_df()
        self.num_processes = self.project_commit_ready_all_df.shape[0]
        # print("num processes: ", self.num_processes)
        for project in self.project_commit_ready_all_df['project'].unique(): # make sure the clone result folder exists
            os.makedirs(os.path.join(config_global.CLONE_RESULT_PATH, project), exist_ok=True)

        executor = cf.ProcessPoolExecutor(max_workers=self.num_workers)
        futures = []
        for row in self.project_commit_ready_all_df.itertuples(index=False):
            future = executor.submit(self.detect_clones_on_commit, row.project, row.commit_id, row.lang)
            futures.append(future)
            # print("================ len futures: ", len(futures))

        # Start the thread for gathering results, should start before the future result is yielding
        gather_thread = threading.Thread(target=self.collect_process_status, daemon=True)
        gather_thread.start()

        for future in cf.as_completed(futures):
            self.completed_futures_result.append(future.result())
        
        gather_thread.join()  # Wait for the gathering thread to finish
        executor.shutdown()
        '''
    """

    def run(self):
        project_commit_status_list_file = os.path.join(config_global.LOG_PATH, 'project_commit_status_list.txt')
        event_handler = FileChangeHandler(project_commit_status_list_file, self.queue)
        observer = Observer()
        observer.schedule(event_handler, path=project_commit_status_list_file, recursive=False)
        observer.start()

        executor = cf.ProcessPoolExecutor(max_workers=self.num_workers)
        futures = []

        while not self.queue.empty():
            project, commit_id, status, size, lang, timestamp = self.queue.get()  # get task from queue
            print("project-commit_id:", project, commit_id, status, size, lang, timestamp)
            if project is None:  # We use (None, None) as a signal of the end of tasks
                break
            future = executor.submit(self.detect_project_commit, project, commit_id, lang)
            
            # If the future is running, then it has definitely started
            if future.running():
                print("Task has started")
            else:
                print("Task has not started yet")

            futures.append(future)

        for future in cf.as_completed(futures):
            print(future.result)
            self.completed_futures_result.append(future.result())

        observer.stop()
        observer.join()
        executor.shutdown(wait=True)
    """


class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, file_path, queue):
        self.file_path = file_path
        self.queue = queue
        self.current_position = 0
        print("File initialized")

    def on_modified(self, event):
        with open(self.file_path, 'r') as file:
            file.seek(self.current_position)
            new_lines = file.readlines()
            
            if new_lines:
                for line in new_lines:
                    line = line.strip()  # remove leading/trailing whitespace
                    if line:  # if line is not empty
                        try:
                            line_dict = json.loads(line)
                            self.queue.put((line_dict["project"], line_dict["commit_id"], line_dict["status"], line_dict["size"], line_dict["lang"], line_dict["timestamp"]))
                        except json.JSONDecodeError:
                            print(f"Could not parse line as JSON: {line}")
            self.current_position = file.tell()    



def checkout_commits_detect_clones(project): # should not get_checkout_commit, since each commit will affact the working env of next commit
    # prepare clone result path
    programming_lang = Git_repo.get_programming_language(project)
    if not programming_lang:
        print("language not detected")
        sys.exit(-1)

    clone_result_path = os.path.join(config_global.CLONE_RESULT_PATH, project)
    os.makedirs(clone_result_path, exist_ok=True) #
    
    # load the status file into memory for one project, create df with column name, empty df.
    nicad_workdir = os.path.join(config_global.REPO_PATH, f'nicad_workdir_{project}')
    project_repo_path = os.path.join(nicad_workdir, project)
    Git_repo.gitclone_repo(project, project_repo_path)
    
    commits_to_run = Producer.get_commits_to_run(project, programming_lang)# [:20] # for commit_id in tqdm(project_commit_status_df['commit_id']):
    # print(f"len commits to run: {commits_to_run}")

    # clean up the checkout working directory
    cmd_clean_nicad_workdir = 'rm -rf %s_*'% project_repo_path
    child = subprocess.Popen(cmd_clean_nicad_workdir, shell=True)
    child.poll()

    for commit_id in tqdm(commits_to_run, desc=f"Checkout commits for {project}"): # [todo] time from oldest to newest, if checkout fails, skip it and record it as errored instead of ready in status
        cmd_git_checkout_commit = ['git', 'checkout', '-f', commit_id] 
        execution_checkout = subprocess.run(cmd_git_checkout_commit, cwd=project_repo_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Git_repo.git_checkout_commit(project, commit_id)
        if execution_checkout.returncode == 0:
            # perform clone detection by NiCad
            cmd_detect_clones = ['nicad6', 'functions', programming_lang, project_repo_path]
            execution_detect_clones = subprocess.run(cmd_detect_clones, cwd=project_repo_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # the make nicad6 detect source code on the cwd
            nicad_clone_file_path = os.path.join(f'{project_repo_path}_functions-blind-clones', f'{project}_functions-blind-clones-0.30-classes-withsource.xml') # check
            
            if execution_detect_clones.returncode == 0: #os.path.exists(nicad_clone_file_path):
                # move the results to the result folder
                try:
                    # move the results to the result folder
                    if os.path.exists(nicad_clone_file_path):
                        clone_file_dest_path = os.path.join(config_global.CLONE_RESULT_PATH, project, f'{commit_id}.xml')
                        shutil.move(nicad_clone_file_path, clone_file_dest_path)

                        # cleanup the nicad working directory or use nicad cleanall, be care of the wildcard
                        nicad_workdir_output_pattern = f'{project}_functions*'
                        nicad_workdir_output_pattern_path = os.path.join(nicad_workdir, nicad_workdir_output_pattern)
                        # cmd_clean_nicad_workdir = ['rm', '-rf', f'{nicad_workdir}/{project}_{commit_id}_functions*']
                        # child = subprocess.Popen(cmd_clean_nicad_workdir, shell=True)
                        # child.poll()
                        globbed_paths = glob(nicad_workdir_output_pattern_path)
                        for path in globbed_paths:
                            os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
                    else:
                        print(f"{nicad_clone_file_path} not exists!")
                except Exception as e:
                    print(f"{nicad_clone_file_path} exists, Move clone-detection result operation failed: {e}")
                    sys.exit(-1)
            elif execution_detect_clones.returncode == 99: # there is no java file in the repo
                print(f"no code files exists, Move clone-detection result operation failed: {e}")
                continue
            else:
                print(f"nicad detect clones on {project}-{commit_id} failed")
                sys.exit(-1)
        else:
            print(f"checkout {project}-{commit_id} failed")
            #self.logger.error(f'\t\t rsync out ({project}-{commit_id}) failed - {execution_rsync.stderr}')
            sys.exit(-1)
            

if __name__ == "__main__":

    projects_all = list(config_global.SUBJECT_SYSTEMS_YOUNG.keys()) + list(config_global.SUBJECT_SYSTEMS_MIDDLE.keys()) + list(config_global.SUBJECT_SYSTEMS_OLD.keys())
    projects = ['jabref','framework', 'gatk', 'WinObjC', 'druid']
    projects_detected = ['graylog2-server', 'Terasology', 'janusgraph'] # 'systemd', 
    projects_problematic = ['systemd']
    
    projects_check = list(set(projects_all) - set(projects))
    projects_check = ['janusgraph', 'graylog2-server', 'john', 'Terasology', 'druid', 'systemd', 'jabref']
    print(len(projects_all), len(projects), len(projects_check))
    projects_check = ['systemd', 'john', 'jabref']
    projects = ['systemd', 'jabref']
    projects = ['FreeRDP']
    projects = ['inav']
    print("len to check: ", len(projects_check))

    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=['producer', 'consumer', 'combiner', 'check'], help='Run the script as a producer or consumer or combiner')
    args = parser.parse_args()
    if args.role == 'producer':
        producer = Producer(projects)
        producer.run()
    elif args.role == 'consumer':
        consumer = Consumer()
        consumer.run()
    elif args.role == 'combiner':
        print("projects: ", projects)
        with cf.ProcessPoolExecutor(max_workers=len(projects) + 1) as executor:
            futures = [executor.submit(checkout_commits_detect_clones, project) for project in projects]      
    elif args.role == 'check':
        for project in projects: # projects_todo + projects_problematic:
            clone_result_dir = os.path.join(config_global.CLONE_RESULT_PATH_DATADIR, project)
            # clone_result_dir = os.path.join(config_global.CLONE_RESULT_PATH, project)
            os.makedirs(clone_result_dir, exist_ok=True) # cmd_mkdir_clone_result = 'mkdir -p %s/%s' % (config_global.CLONE_RESULT_PATH, project)
            clone_result_extracted = [x.split(".")[0] for _, _, files in os.walk(clone_result_dir) for x in files] # remove the posix 
    
            commit_log_df = Git_repo.get_commit_log_df(project)
            commits_to_run = list(set(commit_log_df['commit_id'].tolist()) - set(clone_result_extracted))
            print(f"{project} len commits_to_run: {len(commits_to_run)}")
            
            # checkout_commits_detect_clones(project, commits_to_run) 
    else:
        sys.exit(-1)
