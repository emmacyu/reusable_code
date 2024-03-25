import os, sys, csv, re, time, json, requests, configparser, pickle, argparse, subprocess, logging, math, yaml, itertools
sys.path.append(".")
sys.path.append("..")
from datetime import datetime
import pandas as pd
from tqdm import tqdm
from github import Auth, Github
from time import time, strftime, localtime

from config import config_global
from utils import Git_repo
import concurrent.futures as cf
from functools import partial
tqdm.pandas() # Enable progress_apply on DataFrame
# import dask.dataframe as dd

sys.path.insert(0, os.path.join(config_global.TOOLS_PATH, 'pyszz_v2'))
print(config_global.TOOLS_PATH)
# sys.path.insert(1, os.path.join(config_global.TOOLS_PATH, 'pyszz_v2', 'szz'))
# from tools.pyszz_v2 import conf, szz
#from tools.pyszz_v2  import main as pyszz
#import tools.pyszz_v2.main as pyszz
#import tools.pyszz_v2.szz as szz
#from tools.pyszz_v2 import *
# from tools.pyszz_v2 import main
# from tools.pyszz_v2 import conf as szz_conf


# take care of buggy code that is identified by the commit messages which provide bug information.
class Buggy_commits_extractor:

    def __init__(self, project):
        self._config_logger()
        self._config_szz_workdir(project)
        self.gitlog_pattern = config_global.BUGFIX_PATTERN
        self.project = project


    def _config_szz_workdir(self, project):
        owner, repo_name = config_global.SUBJECT_SYSTEMS_ALL[project].split("/")
        self.szz_repo_path = os.path.join(config_global.REPO_PATH, 'szz_workdir', owner, repo_name)
        Git_repo.gitclone_repo(project, self.szz_repo_path)


    def _config_logger(self):
        # configure logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        # Create a file handler
        log_dir = os.path.join(config_global.LOG_PATH, config_global.SERVER_NAME)
        os.makedirs(log_dir, exist_ok=True)
        now = strftime('%Y-%m-%d-%H:%M:%S', localtime(time()))
        log_file_path = os.path.join(log_dir, f'{Buggy_commits_extractor.__name__}_running_{now}.log')
        #if os.path.exists(log_file_path):
            #os.remove(log_file_path)
        
        handler = logging.FileHandler(log_file_path)
        handler.setLevel(logging.INFO)
        
        # Create a logging format
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)


    # extract bugfix commits by pull requests: get the bug-fixing commits directly from pr
    def extract_bugfix_commits_by_pr(self, project, issue_config_dict, git_repo):
        # get all the pull request related to bugs
        import requests, json
        token = 'xxx'
        uname = 'yiping-jia'
        params = {'state': 'closed', 'labels': 'bug', 'per_page': 100, 'page': 1}
        url = "https://api.github.com/repos/broadinstitute/gatk/pulls"
        print(url)
        response = requests.get(url, auth=(uname, token), params=params)
        response_json = json.loads(response.text)
        print(len(response_json))
        bug_pr_list = []
        while len(response_json) > 0:
            params['page'] = params['page'] + 1
            response = requests.get(url, auth=(uname, token), params=params)
            response_json = json.loads(response.text)
    
            for i in response_json:
                if len(i['labels']) > 0:
                    for b in i['labels']:
                        if b['name'] == "bug":
                            bug_pr_list.append(i)
        print("done")
    
        # retrieve the bug-fix commits
        temp_dict = dict()
        temp_dict['project'] = "gatk"
        temp_dict['bug info'] = []
    
        bugfix_commit_list = []
        for bug_pr in bug_pr_list:
            pretty_string = json.dumps(bug_pr, indent=4)
            temp_dict['bug info'].append(pretty_string)
            commit = bug_pr['head']['sha'][:7]
            bugfix_commit_list.append(commit)
    
    
    # extract bugfix commits by commit messages, commits that has fix issue_no words in its commit messages are fixing commits.
    def extract_bugfix_commits_by_msg(self, commits_log_df):
        bugfix_commit_list = []
        for index, row in commits_log_df.iterrows():
            # bugs = re.findall(config_global.BUGFIX_PATTERN, row['commit_msg'], re.IGNORECASE)
            bugs = []
            try:
                bugs = re.findall(config_global.BUGFIX_PATTERN, row['commit_msg'], re.IGNORECASE)
            except:
                pass

            if len(bugs):
                bugfix_commit_list.append(row['commit_id'])
                #print(row['commit_msg'])
    
        return bugfix_commit_list # 去重


    def _get_opendate_by_bugid(self, bug_id, issue_tracker, issue_link, creation_flag, error_flag):
        time.sleep(1)
        opened_date = None
        urlItem = None
        opened_date, fixed_date = None, None
        if issue_tracker == 'jira':
            url = issue_link % (bug_id, bug_id)
        else:
            #url = issue_link % bug_id
            url = os.path.join(issue_link, bug_id)
        url = url.replace("//github.com", "//api.github.com/repos")
    
        try:
            #urlItem = urllib2.urlopen(issue_link)
    
            api_token = "xxx"
            headers = {'Authorization': 'token %s' % api_token}
            #breakpoint()
            response = requests.get(url)
    
            if response.status_code == 200:
                issue_page = response.json()
                open_date = datetime.strptime(issue_page['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y%m%d')
                return open_date
    
        except:
            #logging.info("{a}: wrong bug link!".format(bug_id))
            print("wrong bug id %s" % bug_id)
    
        return None


    # extract bugfix commits either by github issues or bugzilla platform
    def extract_bugfix_commits_by_issues_test(self, project, issue_config_dict, git_repo):
        bug_commit_mapping = dict()
    
        # df_commit_logs = pd.read_csv(log_file, header=None, sep='|')
        df_commit_logs = pd.DataFrame(columns=['commit_id', 'email', 'commit_timestamp', 'commit_msg'])
        commits_log_path = os.path.join(config_global.COMMIT_LOG_PATH, "%s_commits.log" % project)
        commits_log_list = list()  # pd.DataFrame()
        with open(commits_log_path, 'r', encoding='utf-8') as fp: # be careful of the encoding
            reader = fp.readlines()
            for line in reader:
                commits_log_list.append(line.split(',', 3))
    
        commits_log_df = pd.DataFrame(commits_log_list, columns=['commit_id', 'email', 'timestamp', 'commit_msg'])
        commits_log_df.sort_values('timestamp', inplace=True)
        # commits_log_df = commits_log_df[5000:]
        print(commits_log_df.shape)
        all_bugs = set()
    
        bug_inducing_list = []
    
        # read from commit log with commit messages
        commits_log_path = os.path.join(config_global.COMMIT_LOG_PATH, '%s_logs.txt' % project)
        commits_log_df = pd.read_csv(commits_log_path, header=None, names=['commit_id', 'email', 'timestamp', 'commit_msg'])
    
        for index, row in commits_log_df.iterrows():
            bugs = re.findall(config_global.BUGFIX_PATTERN, )
    
            if issue_config_dict['tracker'] == 'bugzilla':
                bugs = re.findall(issue_config_dict['pattern'], row['commit_msg'], re.IGNORECASE)
            else:
                bugs = re.findall(issue_config_dict['pattern'], row['commit_msg'], re.I)
    
            if bugs:
                commit_id = row['commit_id']
                commit = git_repo.get_commit(commit_id)
                commits_related = git_repo.get_commits_last_modified_lines(commit)
                commits_related_buggy = dict(filter(lambda modified_file: modified_file[0].endswith('.java'), commits_related.items()))
                for modified_file in commits_related_buggy:
                    for candidate_commit_id in commits_related_buggy[modified_file]:
                        bug_inducing_list.append([candidate_commit_id, commit_id])
    
        df = pd.DataFrame(bug_inducing_list, columns=['inducing_commit', 'fixing_commit']).drop_duplicates()
        szz_path = os.path.join(config_global.OUTPUT_PATH, 'szz', '%s_fault_inducing.csv' % project)
        df.to_csv(szz_path, index=False)
        # filter buggy_commits if time permits
    
        return bug_commit_mapping
    
    
    def get_bugfix_commits(project):
        # read in project commit file with commit messages
        commits_log_path = os.path.join(config_global.COMMIT_LOG_PATH, '%s_logs.txt' % project)
        commits_log_df = pd.read_csv(commits_log_path, header=None,
                                     names=['commit_id', 'email', 'timestamp', 'commit_msg'])
        print(commits_log_df.shape)
        # get bug fixing commits
        bugfix_commit_list = extract_bugfix_commits_by_msg(commits_log_df)
        print("len bugfix commit mapping: ", len(bugfix_commit_list))
    
        # get bug inducing commits by szz
        buggy_commit_list = extract_buggy_commits_szz(project, bugfix_commit_list)
    

    def extract_bugfix_commit_by_issue(self, row):
        issue_no = row['number']
        bugfix_commit_candidates = []
        for commit in self.commit_log_msg_list:
            """ a commit example
            commit:  commit b24581f9bde1d961f034f9915cd84821d375d2e5
            Author: sparked435 <9814958+sparked435@users.noreply.github.com>
            Date:   2023-04-24 20:47:46 -0400

                Running contrib/format.sh.
            """

            pattern = self.gitlog_pattern.format(nbr=issue_no)
        
            # find fix and issue number in the commit message
            #if re.search(r'#\s*{nbr}\D'.format(nbr=issue_no), commit) and re.search('fix|resolve', commit, re.IGNORECASE):
            if re.search(r'#{nbr}\D'.format(nbr=issue_no), commit) and re.search('fix|solve', commit, re.IGNORECASE):
                bugfix_commit_candidates.append(commit)
        
        #filtered_commits = [commit for commit in bugfix_commit_candidates if not re.search('[Mm]erge|[Cc]herry|[Nn]oting', commit)] # not merge pull like "Merge pull request #2 from mfournier/amqp-symbol_lookup\n"
        #bugfix_commit_candidates_filtered = list(filter(lambda commit: not re.search('[Mm]erge', commit), bugfix_commit_candidates))  # remove pull request merges
        self.logger.info(f"\t\t issue_no {issue_no} has the bugfix_commit_candidates: [{bugfix_commit_candidates}]")
        if bugfix_commit_candidates:
            latest_commit = bugfix_commit_candidates[0]  # the commits are already ordered
            fix_commitid = re.search('(?<=^commit )[a-z0-9]+(?=\n)', latest_commit).group(0) # commitid, author, date, msg = commit.split("\n", 3)
            fix_timestamp = re.search('(?<=\nDate:)[0-9 -:+]+(?=\n)', latest_commit).group(0)
            return (issue_no, fix_commitid, fix_timestamp)
        else:
            return (issue_no, None, None)


    def match_issue_with_bugfix_commitid(self, commit_log_msg_list, issues_df):
        fixed_issues_df_path = os.path.join(config_global.DATA_PATH, 'issues', f'{self.project}_fixed_issues_df.csv')
        if os.path.exists(fixed_issues_df_path):
            # print(f"file exists: {fixed_issues_df_path}")
            fixed_issues_df = pd.read_csv(fixed_issues_df_path)
            return fixed_issues_df
        else:
            bugfix_pattern = re.compile(r'(fix|solve)', re.IGNORECASE)
            commit_log_msg_list_filtered = [commit for commit in commit_log_msg_list if bugfix_pattern.search(commit)]
            print(f"commit log msg list - before & after: {len(commit_log_msg_list)} & {len(commit_log_msg_list_filtered)}")
            self.logger.info(f"commit log msg list - before & after: {len(commit_log_msg_list)} & {len(commit_log_msg_list_filtered)}")
            # pattern = re.compile(r'commit (?P<commit_id>[a-f0-9]{40}).*?Date:\s*(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \+\d{4}).*?(?P<issue>#\s*\d+)', re.DOTALL)
            pattern = re.compile(r'commit (?P<bugfix_commitid>[a-f0-9]{40}).*?Date:\s*(?P<timestamp>(?<=\nDate:)[0-9 -:+]+(?=\n)).*?(?P<issue>#\s*\d+)', re.DOTALL)
            issue_pattern = re.compile(r'#\s*(\d+)')
    
            issue_commitid_matches = []
            # re.search(r'#{nbr}\D'.format(nbr=issue_no), commit) and re.search('fix|solve', commit, re.IGNORECASE)
            for commit_msg in commit_log_msg_list_filtered:
                match = pattern.search(commit_msg)
                if match:
                    commit_id = match.group('bugfix_commitid')
                    timestamp = match.group('timestamp')
                    issue_matches = issue_pattern.findall(commit_msg)
                    if issue_matches:
                        for issue in issue_matches:
                            issue_commitid_matches.append([int(issue), commit_id, pd.to_datetime(timestamp, utc=True)])
            
            if issue_commitid_matches:
                fixed_issues_df = pd.DataFrame(issue_commitid_matches, columns=['issue_number', 'bugfix_commitid', 'timestamp'])
                print("fixed_issues_df: ", fixed_issues_df)
                fixed_issues_df_valid = fixed_issues_df[fixed_issues_df['issue_number'].isin(issues_df['issue_number'])]
    
                # get the latest commit_id as the bug-fix commit_id for each issue
                fixed_issues_df_valid.loc[:, 'timestamp'] = pd.to_datetime(fixed_issues_df_valid['timestamp'])
                print(fixed_issues_df_valid.dtypes, fixed_issues_df_valid.head(5))
                idx = fixed_issues_df_valid.groupby(['issue_number'])['timestamp'].idxmax()
                fixed_issues_df_latest = fixed_issues_df_valid.loc[idx]
                fixed_issues_df_latest = fixed_issues_df_latest.merge(issues_df[['issue_number', 'issue_date']], on='issue_number', how='left')
                #fixed_issues_df_latest['issue_date'] = pd.to_datetime(fixed_issues_df_latest['issue_date'])
                #fixed_issues_df_latest['issue_date'] = fixed_issues_df_latest['issue_date'].dt.strftime('%Y-%m-%dT%H:%M:%S')

                fixed_issues_df_latest.to_csv(fixed_issues_df_path, index=False)
                return fixed_issues_df_latest
            else:
                self.logger.error(f"fail to get bugfix commits with issues")
    

    def _extract_buggy_commits_by_bugfix_commit(self, git_repo, bugfix_commit):
        return bugfix_commit, git_repo.get_commits_last_modified_lines(git_repo.get_commit(bugfix_commit))

    
    # extract bug inducing commits by SZZ algorithm based on the extracted bugfix commits
    def extract_buggy_commits_szz(self, project, bugfix_commit_list):
        # 
        commit_bugfix_buggy_mapping_path = os.path.join(config_global.DATA_PATH, 'raszz_manual_analysis_200', f'{project}_commit_bugfix_buggy_mapping_dict.pkl')
        if os.path.exists(commit_bugfix_buggy_mapping_path):
            with open(commit_bugfix_buggy_mapping_path, 'rb') as fp:
                commit_bugfix_buggy_mapping = pickle.load(fp)
                return commit_bugfix_buggy_mapping
        else:
            self.logger.info(f"{project} bugfix_commit_list \n\t: len: {len(bugfix_commit_list)} {bugfix_commit_list[:5]}")
            project_local_repo = os.path.join(config_global.REPO_PATH, 'nicad_workdir_%s' % project, project)
            
            from pydriller import Git
            git_repo = Git(project_local_repo)  # gr = Git('test-repos/test5')
            
            with cf.ProcessPoolExecutor() as executor:
                # results = list(executor.map(self._extract_buggy_commits_by_bugfix_commit, bugfix_commit_list))
                fn = partial(self._extract_buggy_commits_by_bugfix_commit, git_repo)
                results = list(executor.map(fn, bugfix_commit_list))
            
            commit_bugfix_buggy_mapping = dict(results)
    
            with open(commit_bugfix_buggy_mapping_path, 'wb') as fp:
                pickle.dump(commit_bugfix_buggy_mapping, fp)
            return commit_bugfix_buggy_mapping


    def prepare_bugfixes_json_for_raszz(self, project, fixed_issues_df):
        print(fixed_issues_df.head(5))

        # debug
        bug_fixes_json_path = os.path.join(config_global.DATA_PATH, 'raszz_manual_analysis_200', f'{project}_bug_fixes.json')
        if os.path.exists(bug_fixes_json_path):
            return
        else:
            if fixed_issues_df.empty:
                return 
            
            # lang = Git_repo.get_programming_language(project)
            bug_fixes_json = [{
                "repo_name": config_global.SUBJECT_SYSTEMS_ALL[project],
                "fix_commit_hash": row['bugfix_commitid'],
                "earliest_issue_date": str(row['issue_date'])
                # "language": [lang]
                }    
                for _, row in fixed_issues_df.iterrows()
            ]
            
            with open(bug_fixes_json_path, 'w') as fp:
                json.dump(bug_fixes_json, fp, indent=4)  # use `indent` to format the output


    def extract_buggy_commits_raszz(self, project):

        # debug
        raszz_result_path = os.path.join(config_global.DATA_PATH, 'raszz_manual_analysis_200', f'{project}_bic_raszz_for_manual_analysis.json')
        if not os.path.exists(raszz_result_path):
            # check pydriller version and lower pydriller version to 1.15 if not

            # debug
            bug_fixes_json_path = os.path.join(config_global.DATA_PATH, 'raszz_manual_analysis_200', f'{project}_bug_fixes.json')
            with open(bug_fixes_json_path, 'r') as f:
                data = json.load(f)
                num_lines = len(data)
                print(f'The number of lines in the bug fix JSON file is: {num_lines}')
            
            szz_workdir = os.path.join(config_global.REPO_PATH, 'szz_workdir')
            print("szz_workdir: ", szz_workdir)
            szz_conf_path = os.path.join(config_global.TOOLS_PATH, 'pyszz_v2', 'conf', 'raszz.yml')
            print("szz conf path: ", szz_conf_path)
            with open(szz_conf_path, 'r') as f:
                szz_conf = yaml.safe_load(f)
                print("***", szz_conf, type(szz_conf))
                pyszz.main(bug_fixes_json_path, raszz_result_path, szz_conf, szz_workdir)
            # upgrade pydriller version back to 2.5


    def identify_buggy_commits(self, project):
        # debug
        raszz_result_path = os.path.join(config_global.DATA_PATH, 'raszz_manual_analysis_200', f'{project}_bic_raszz_for_manual_analysis.json')
        if os.path.exists(raszz_result_path):
            with open(raszz_result_path, 'r') as fp:
                raszz_result = json.load(fp)
                buggy_commit_list_flat = [buggy_commit for bugfix_item in raszz_result for buggy_commit in bugfix_item['inducing_commit_hash']]
            return buggy_commit_list_flat

        # debug
        buggy_commit_list_path = os.path.join(config_global.DATA_PATH, "raszz_manual_analysis_200", "%s_buggy_commits.pkl" % project)
        if os.path.exists(buggy_commit_list_path):
            print(f"file exists: {buggy_commit_list_path}")
            with open(buggy_commit_list_path, 'rb') as fp:
                buggy_commit_list_flat = pickle.load(fp)
                return buggy_commit_list_flat
        else:
            print(f"file not exists: {buggy_commit_list_path}")
            # load github issues
            issues_df = Git_repo.get_github_issues(project)
            print(f"issues for {project}: {issues_df.shape} {issues_df.columns} \n {issues_df.head(1)}")
            self.logger.info(f"issues for {project}: {issues_df.shape} {issues_df.columns} \n {issues_df.head(1)}")
    
            # load github commit messages
            self.commit_log_msg_list = Git_repo.get_commit_log_msg(project, self.szz_repo_path)
            if not self.commit_log_msg_list:
                sys.exit(-1)
            self.logger.info(f"# commits for {project}: {len(self.commit_log_msg_list)}, \n {self.commit_log_msg_list[0]}")

            # find bug fixes based on issues and commit messages using RA-SZZ
            fixed_issues_df = self.match_issue_with_bugfix_commitid(self.commit_log_msg_list, issues_df) # fixed_issues_df = self.match_bugfix_commits_with_issues(project, issues_df, self.commit_log_msg_list, config_global.BUGFIX_PATTERN)
            print(fixed_issues_df.columns)
            self.logger.info(f"finish extracting fixed_issues_df: {fixed_issues_df.shape} {fixed_issues_df.columns} \n {fixed_issues_df.head(5)}")
            # bugfix_commit_list = list(fixed_issues_df['bugfix_commitid'].unique())

            Git_repo.gitclone_repo(project, self.szz_repo_path)
            self.prepare_bugfixes_json_for_raszz(project, fixed_issues_df)
            # self.prepare_bugfixes_json_for_raszz(project, bugfix_commit_list)
            self.extract_buggy_commits_raszz(project)

            with open(raszz_result_path, 'r') as fp:
                raszz_result = json.load(fp)
                buggy_commit_list_flat = [buggy_commit for bugfix_item in raszz_result for buggy_commit in bugfix_item['inducing_commit_hash']]
            
            # get bug inducing commits
            # commit_bugfix_buggy_mapping = self.extract_buggy_commits_szz(project, bugfix_commit_list)
            # buggy_commits_list =  [modified_files[file] for bugfix_commit, modified_files in commit_bugfix_buggy_mapping.items() for file in modified_files]
            # buggy_commit_list_flat = list(set([item for subset in buggy_commits_list for item in subset]))
            # self.logger.info(f"finish extracting bug-inducing commits")

            with open(buggy_commit_list_path, 'wb') as fp:
                pickle.dump(buggy_commit_list_flat, fp)
            return buggy_commit_list_flat


    def identify_bugfix_commits(self, project):
        # load github issues
        issues_df = Git_repo.get_github_issues(project)
        # print(f"issues for {project}: {issues_df.shape} {issues_df.columns} \n {issues_df.head(1)}")
        self.logger.info(f"issues for {project}: {issues_df.shape} {issues_df.columns} \n {issues_df.head(1)}")

        # load github commit messages
        self.commit_log_msg_list = Git_repo.get_commit_log_msg(project, self.szz_repo_path)
        if not self.commit_log_msg_list:
            sys.exit(-1)
        self.logger.info(f"# commits for {project}: {len(self.commit_log_msg_list)}, \n {self.commit_log_msg_list[0]}")
        
        # find bug fixes based on issues and commit messages using RA-SZZ
        fixed_issues_df = self.match_issue_with_bugfix_commitid(self.commit_log_msg_list, issues_df) # fixed_issues_df = self.match_bugfix_commits_with_issues(project, issues_df, self.commit_log_msg_list, config_global.BUGFIX_PATTERN)
        # print(fixed_issues_df.columns)
        self.logger.info(f"finish extracting fixed_issues_df: {fixed_issues_df.shape} {fixed_issues_df.columns} \n {fixed_issues_df.head(5)}")
        bugfix_commit_list = list(fixed_issues_df['bugfix_commitid'].unique())
        
        #print("# bugfix commits / # commits for ", project, ":", len(bugfix_commit_list), '/', len(self.commit_log_msg_list))
        print(f"# bugfix commits / # commits for {project:>26}: {len(bugfix_commit_list):>6} / {len(self.commit_log_msg_list):>6}")
        return bugfix_commit_list
        


def anatomize_genealogy(genealogy_df, commit_time_dict, commit_author_dict):
    genealogy_df['n_siblings_start'] = genealogy_df['clone_group_tuple'].map(lambda x: len(x.split('|')))
    genealogy_df['genealogy_list'] = genealogy_df['genealogy'].map(lambda gen: gen.split(';'))
    genealogy_df['commit_list'] = genealogy_df['genealogy_list'].map(lambda gen: [x.split(":")[0] for x in gen])
    # genealogy_df['churn'] = genealogy_df['genealogy_list'].map(lambda gen: sum([int(x.split(":")[1]) for x in gen]))
    
    #print("================", genealogy_df['churn'])
    genealogy_df.drop(['genealogy'], axis=1, inplace=True)
    genealogy_df['n_genealogy'] = genealogy_df['commit_list'].map(lambda x: len(x))
    
    # each commit in genealogy: commit_id: #updates: clone_group_sig
    genealogy_df['end_commit'] = genealogy_df['commit_list'].map(lambda x: x[-1])
    #genealogy_df['end_clone_group_tuple'] = genealogy_df['genealogy_list'].map(lambda x: x[-1].split(':', 2)[2])
    #genealogy_df['n_siblings_end'] = genealogy_df['end_clone_group_tuple'].map(lambda x: len(x.split(',')))

    
    # calculate duration in terms of days
    genealogy_df['n_days'] = list(map(lambda x, y: (commit_time_dict[y] - commit_time_dict[x]).days,
                                      genealogy_df['start_commit'], genealogy_df['end_commit']
                                      )
                                  )  # git_repo.get_commit(commit_id).committer.email for commit_id in gen.split('-')
    genealogy_df['start_timestamp'] = genealogy_df['start_commit'].map(lambda x: commit_time_dict[x])  # get timestamp for time-series training
    
    #print("start time xxxxxxxxxxxxxxxxxxxxxxxxxxx: ", genealogy_df.start_timestamp)
    # calucate #authors

    # genealogy_df['genealogy_list'].to_csv("bad.txt")
    genealogy_df['author_list'] = genealogy_df['genealogy_list'].map(
            lambda gen: set([commit_author_dict[commit.split(':', 2)[0]] for commit in gen])
        )

    genealogy_df['n_authors'] = genealogy_df['author_list'].map(lambda x: len(x))
    genealogy_df['cnt_siblings'] = genealogy_df['genealogy_list'].map(lambda gen: max([int(x.split(":")[2]) for x in gen]))
    #genealogy_df['cnt_siblings'] = genealogy_df['genealogy_list'].map(lambda gen: max([len(x.split(':')[3].split('|')) for x in gen]))
    # print(list(genealogy_df['cnt_siblings']))
    # reorder the columns
    #print(genealogy_df.columns)
    # print("before drop duplicates: ", genealogy_df.shape)

    '''
    # calucate #updators
    genealogy_df['updator_list'] = genealogy_df['genealogy_list'].map(
        lambda gen: set([commit_author_dict[commit.split(':', 2)[0]] for commit in
                         (filter(lambda c: c.split(':', 2)[1] != '0', gen))
                         ])
    )
    genealogy_df['n_updators'] = genealogy_df['updator_list'].map(lambda x: len(x))
    genealogy_df['n_genealogy_updated'] = genealogy_df['genealogy_list'].map(
        lambda gen: len(list(filter(lambda c: c.split(':', 2)[1] != '0', gen)))
    )
    '''
    # drop duplicates
    genealogy_df['clone_group_tuple'].drop_duplicates(inplace=True)
    #print("after drop duplicates: ", genealogy_df.shape)
    #print("==================================")
    return genealogy_df


# rank by longevity: if longevous then 1 else 0
def rank_by_lifecycle(genealogy_df, is_by_genealogy, threshold=0.5): # if is_by_genealogy, rank by length of commit genealogy, otherwise by number of days
    '''
        genealogy_df['rank_by_n_genealogy'] = genealogy_df['n_genealogy'].map(
                lambda x: 1 if x > genealogy_df.n_genealogy.quantile(0.75) else (
                    0 if x < genealogy_df.n_genealogy.quantile(0.25) else -1))
    '''
    
    genealogy_df['rank_by_n_genealogy'] = genealogy_df['n_genealogy'].map(
        lambda x: 1 if x >= genealogy_df.n_genealogy.quantile(threshold) else 0)
    
    genealogy_df['rank_by_n_days'] = genealogy_df['n_days'].map(
        lambda x: 1 if x >= genealogy_df['n_days'].quantile(threshold) else 0)

    if is_by_genealogy:
        genealogy_df['rank_by_lifecycle'] = genealogy_df['rank_by_n_genealogy']
    else:
        genealogy_df['rank_by_lifecycle'] = genealogy_df['rank_by_n_days']


def rank_by_prevalence(genealogy_df, threshold=0.5):
    #genealogy_df['rank_by_prevalence'] = genealogy_df['n_siblings_start'].map(lambda x: 1 if x > genealogy_df.n_siblings_start.quantile(0.5) else 0)

    genealogy_df['rank_by_prevalence'] = genealogy_df['cnt_siblings'].map(
        lambda x: 1 if x >= genealogy_df.cnt_siblings.quantile(threshold) else 0)
    
    #print("rank_by_prevalence: ", genealogy_df['rank_by_prevalence'].value_counts())
    #print("----------------")
    #genealogy_df['rank_by_n_authors'] = genealogy_df['rank_by_n_authors'].map({1: 'high', 0: 'low'})  # 'volvo':0 , 'bmw':1, 'audi':2} )


def get_intersection(gen, buggy_commit_list):
    matches = []
    for short_commit in tqdm(gen, desc='gen: '):
        for long_commit in buggy_commit_list:
            if long_commit.startswith(short_commit):
                matches.append(short_commit)
    return matches


# to retrieve the bug-proneness info: # bug-inducing commits / # normal commits
def rank_by_bugproneness(genealogy_df, buggy_commit_list, threshold=0.5):

    # look into the distribution of n_genealogy
    #segments = pd.cut(genealogy_df['n_genealogy'], bins=[0,2,5,100,1000])
    #counts = pd.value_counts(segments, sort=True)
    #print(genealogy_df['n_genealogy'].value_counts())
    #print(counts)

    buggy_commit_list = list(set(buggy_commit_list))
    #print("genealogy columns: ", genealogy_df.columns)

    #genealogy_df['buggy_genealogy'] = genealogy_df['commit_list'].map(lambda gen: list(set(gen).intersection(set(buggy_commit_list))))
    genealogy_df['buggy_genealogy'] = genealogy_df['commit_list'].map(lambda gen: list(set(gen).intersection(set(buggy_commit_list))))
    # Applying the function
    #genealogy_df['buggy_genealogy'] = genealogy_df['commit_list'].apply(get_intersection, args=(buggy_commit_list,))
    
    genealogy_df['n_buggy_genealogy'] = genealogy_df['buggy_genealogy'].map(lambda gen: len(gen))
    genealogy_df['bug_proneness'] = genealogy_df.apply(lambda row:  (row['n_genealogy'] - row['n_buggy_genealogy']) / (row['n_buggy_genealogy'] if row['n_buggy_genealogy'] != 0 else 1), axis=1)
    
    genealogy_df['rank_by_bugproneness'] = genealogy_df['bug_proneness'].map(
        lambda x: 1 if x >= genealogy_df.bug_proneness.quantile(threshold) else 0)

    print(genealogy_df['rank_by_bugproneness'].value_counts())
    # look into the distribution of bug_proneness
    segments = pd.cut(genealogy_df['bug_proneness'], bins=[0, 2, 5, 10, 100, 10000])
    #print(genealogy_df['rank_by_bugproneness'].value_counts())
    #counts = pd.value_counts(segments, sort=True)
    #print(counts)



def decide_final_label(project, is_by_genealogy, is_longevous, is_prevalent, is_buggy):

    genealogy_path = os.path.join(config_global.GROUP_GENEALOGY_PATH, f'{project}_group_genealogy_distinct.csv')
    if not os.path.exists(genealogy_path):
        return
    
    genealogy_df = pd.read_csv(genealogy_path)

    commits_log_df = Git_repo.get_commit_log_df(project)
    commits_log_df['timestamp'] = pd.to_datetime(commits_log_df['timestamp'], errors='coerce')

    commit_author_dict = dict(zip(commits_log_df.commit_id, commits_log_df.email))
    commit_time_dict = dict(zip(commits_log_df.commit_id, commits_log_df.timestamp))

    genealogy_anatomized_df = anatomize_genealogy(genealogy_df, commit_time_dict, commit_author_dict)

    # rank by 1rd dimension - Clone LifeCycle
    rank_by_lifecycle(genealogy_anatomized_df, is_by_genealogy, config_global.lifecycle_threshold)

    # rank by 2nd dimension - Clone Prevalence
    rank_by_prevalence(genealogy_anatomized_df, config_global.prevalence_threshold)

    # rank by 3rd dimension - Clone Bug-proneness
    buggy_commits_extractor = Buggy_commits_extractor(project)
    bugfix_commit_list = buggy_commits_extractor.identify_bugfix_commits(project)
    print(project, "has bugfix commits: ", len(bugfix_commit_list))
    rank_by_bugproneness(genealogy_anatomized_df, list(set(bugfix_commit_list)), config_global.quality_threshold)

    if is_longevous and is_prevalent and is_buggy: # decide the label by all the three dimensions
        genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
            lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_prevalence + row.rank_by_bugproneness) / 3),
            axis=1
        ).astype(int)
    elif is_longevous and is_buggy:
        genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
            lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_bugproneness) / 2),
            axis=1
        ).astype(int)
    elif is_longevous:
        genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df['rank_by_lifecycle']

    label_path = os.path.join(os.path.normpath(config_global.LABEL_PATH), f'20230912_{project}_3label_{config_global.lifecycle_threshold}_{config_global.prevalence_threshold}_{config_global.quality_threshold}.csv')
    genealogy_anatomized_df.to_csv(label_path, index=False)
    return genealogy_anatomized_df

    #genealogy_anatomized_df = genealogy_anatomized_df[genealogy_anatomized_df.is_reusable != -1]
    #genealogy_df['rank1'] = (
        #genealogy_df.apply(lambda row: math.floor(row.rank_by_n_genealogy + row.rank_by_n_authors), axis=1)).astype(int)


def decide_final_label_at_thresholds(project, is_by_genealogy, is_longevous, is_prevalent, is_buggy):

    genealogy_path = os.path.join(config_global.GROUP_GENEALOGY_PATH, f'{project}_group_genealogy_distinct.csv')

    values = [0.3, 0.4, 0.5, 0.6, 0.7]
    combinations = list(itertools.product(values, repeat=3))
    combinations = [(0.3, 0.3, 0.3), (0.4, 0.4, 0.4), (0.5, 0.5, 0.5), (0.6, 0.6, 0.6), (0.7, 0.7, 0.7)]
    #print(combinations)
    for combo in tqdm(combinations, desc='combinations: '):
        print(combo)
        lifecycle_threshold, prevalence_threshold, quality_threshold = combo[0], combo[1], combo[2]
        label_path = os.path.join(os.path.normpath(config_global.LABEL_PATH), f'20230912_{project}_3label_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}.csv')
        if os.path.exists(label_path):
            continue

        genealogy_df = pd.read_csv(genealogy_path)
    
        commits_log_df = Git_repo.get_commit_log_df(project)
        commits_log_df['timestamp'] = pd.to_datetime(commits_log_df['timestamp'], errors='coerce')
    
        commit_author_dict = dict(zip(commits_log_df.commit_id, commits_log_df.email))
        commit_time_dict = dict(zip(commits_log_df.commit_id, commits_log_df.timestamp))
    
        genealogy_anatomized_df = anatomize_genealogy(genealogy_df, commit_time_dict, commit_author_dict)
    
        # rank by 1rd dimension - Clone LifeCycle
        rank_by_lifecycle(genealogy_anatomized_df, is_by_genealogy, lifecycle_threshold)
    
        # rank by 2nd dimension - Clone Prevalence
        rank_by_prevalence(genealogy_anatomized_df, prevalence_threshold)
    
        # rank by 3rd dimension - Clone Bug-proneness
        buggy_commits_extractor = Buggy_commits_extractor(project)
        bugfix_commit_list = buggy_commits_extractor.identify_bugfix_commits(project)
        print(project, "has bugfix commits: ", len(bugfix_commit_list))
        rank_by_bugproneness(genealogy_anatomized_df, list(set(bugfix_commit_list)), quality_threshold)
    
        if is_longevous and is_prevalent and is_buggy: # decide the label by all the three dimensions
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
                lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_prevalence + row.rank_by_bugproneness) / 3),
                axis=1
            ).astype(int)
        elif is_longevous and is_buggy:
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
                lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_bugproneness) / 2),
                axis=1
            ).astype(int)
        elif is_longevous:
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df['rank_by_lifecycle']

        print(genealogy_anatomized_df['is_reusable'].value_counts())
        genealogy_anatomized_df.to_csv(label_path, index=False)
        

def decide_final_label_at_thresholds2(is_by_genealogy, is_longevous, is_prevalent, is_buggy):
    project_threshold_tuples = ['mpv_3label_20230808_0.7_0.5_0.6.csv', 'druid_3label_20230808_0.6_0.5_0.6.csv', 'mpv_3label_20230808_0.7_0.5_0.7.csv', 'radare2_3label_20230808_0.5_0.6_0.3.csv', 'radare2_3label_20230808_0.6_0.4_0.7.csv', 'druid_3label_20230808_0.7_0.3_0.7.csv', 'radare2_3label_20230808_0.4_0.4_0.7.csv', 'radare2_3label_20230808_0.7_0.7_0.7.csv', 'radare2_3label_20230808_0.4_0.3_0.7.csv', 'betaflight_3label_20230808_0.5_0.4_0.4.csv', 'druid_3label_20230808_0.5_0.5_0.7.csv', 'framework_3label_20230808_0.7_0.7_0.6.csv', 'druid_3label_20230808_0.7_0.7_0.5.csv', 'radare2_3label_20230808_0.5_0.4_0.3.csv', 'druid_3label_20230808_0.5_0.7_0.3.csv', 'mpv_3label_20230808_0.5_0.3_0.6.csv', 'druid_3label_20230808_0.6_0.3_0.5.csv', 'druid_3label_20230808_0.5_0.5_0.4.csv', 'betaflight_3label_20230808_0.5_0.6_0.6.csv', 'framework_3label_20230808_0.5_0.7_0.7.csv', 'mpv_3label_20230808_0.6_0.6_0.5.csv', 'betaflight_3label_20230808_0.6_0.4_0.4.csv', 'mpv_3label_20230808_0.7_0.6_0.4.csv', 'framework_3label_20230808_0.7_0.3_0.6.csv', 'framework_3label_20230808_0.6_0.4_0.7.csv', 'druid_3label_20230808_0.5_0.4_0.3.csv', 'druid_3label_20230808_0.7_0.6_0.5.csv', 'mpv_3label_20230808_0.5_0.5_0.4.csv', 'betaflight_3label_20230808_0.6_0.4_0.7.csv', 'radare2_3label_20230808_0.4_0.7_0.3.csv', 'radare2_3label_20230808_0.4_0.6_0.7.csv', 'radare2_3label_20230808_0.7_0.4_0.7.csv', 'druid_3label_20230808_0.7_0.7_0.7.csv', 'framework_3label_20230808_0.6_0.5_0.4.csv', 'radare2_3label_20230808_0.7_0.5_0.3.csv', 'radare2_3label_20230808_0.6_0.5_0.5.csv', 'framework_3label_20230808_0.5_0.7_0.4.csv', 'radare2_3label_20230808_0.6_0.4_0.5.csv', 'betaflight_3label_20230808_0.7_0.7_0.7.csv', 'radare2_3label_20230808_0.6_0.4_0.4.csv', 'mpv_3label_20230808_0.6_0.4_0.5.csv', 'framework_3label_20230808_0.7_0.5_0.3.csv', 'druid_3label_20230808_0.7_0.6_0.6.csv', 'betaflight_3label_20230808_0.7_0.6_0.6.csv', 'mpv_3label_20230808_0.7_0.6_0.3.csv', 'radare2_3label_20230808_0.4_0.4_0.5.csv', 'framework_3label_20230808_0.7_0.3_0.3.csv', 'druid_3label_20230808_0.6_0.4_0.5.csv', 'framework_3label_20230808_0.6_0.3_0.5.csv', 'radare2_3label_20230808_0.5_0.6_0.7.csv', 'betaflight_3label_20230808_0.5_0.6_0.4.csv', 'druid_3label_20230808_0.5_0.4_0.5.csv', 'betaflight_3label_20230808_0.5_0.4_0.7.csv', 'framework_3label_20230808_0.6_0.6_0.7.csv', 'framework_3label_20230808_0.6_0.4_0.3.csv', 'radare2_3label_20230808_0.6_0.7_0.3.csv', 'mpv_3label_20230808_0.7_0.3_0.7.csv', 'radare2_3label_20230808_0.6_0.6_0.5.csv', 'mpv_3label_20230808_0.7_0.6_0.6.csv', 'betaflight_3label_20230808_0.5_0.5_0.6.csv', 'radare2_3label_20230808_0.5_0.4_0.6.csv', 'radare2_3label_20230808_0.5_0.3_0.7.csv', 'radare2_3label_20230808_0.7_0.5_0.6.csv', 'framework_3label_20230808_0.6_0.6_0.3.csv', 'druid_3label_20230808_0.6_0.6_0.6.csv', 'druid_3label_20230808_0.5_0.7_0.6.csv', 'mpv_3label_20230808_0.5_0.6_0.6.csv', 'mpv_3label_20230808_0.7_0.7_0.6.csv', 'mpv_3label_20230808_0.5_0.4_0.6.csv', 'radare2_3label_20230808_0.3_0.7_0.3.csv', 'mpv_3label_20230808_0.6_0.7_0.7.csv', 'druid_3label_20230808_0.5_0.3_0.7.csv', 'radare2_3label_20230808_0.3_0.7_0.6.csv', 'radare2_3label_20230808_0.4_0.6_0.3.csv', 'radare2_3label_20230808_0.5_0.3_0.3.csv', 'betaflight_3label_20230808_0.6_0.5_0.5.csv', 'framework_3label_20230808_0.7_0.6_0.5.csv', 'druid_3label_20230808_0.4_0.7_0.6.csv', 'radare2_3label_20230808_0.7_0.5_0.7.csv', 'druid_3label_20230808_0.7_0.3_0.6.csv', 'framework_3label_20230808_0.6_0.5_0.3.csv', 'radare2_3label_20230808_0.6_0.3_0.3.csv', 'mpv_3label_20230808_0.5_0.4_0.7.csv', 'mpv_3label_20230808_0.6_0.5_0.6.csv', 'druid_3label_20230808_0.5_0.6_0.3.csv', 'druid_3label_20230808_0.5_0.4_0.7.csv', 'radare2_3label_20230808_0.7_0.6_0.4.csv', 'betaflight_3label_20230808_0.7_0.6_0.3.csv', 'betaflight_3label_20230808_0.5_0.5_0.4.csv', 'betaflight_3label_20230808_0.6_0.7_0.4.csv', 'druid_3label_20230808_0.7_0.7_0.4.csv', 'radare2_3label_20230808_0.5_0.6_0.5.csv', 'mpv_3label_20230808_0.5_0.7_0.5.csv', 'druid_3label_20230808_0.7_0.3_0.4.csv', 'mpv_3label_20230808_0.5_0.7_0.3.csv', 'radare2_3label_20230808_0.6_0.5_0.4.csv', 'framework_3label_20230808_0.5_0.7_0.3.csv', 'framework_3label_20230808_0.5_0.6_0.5.csv', 'framework_3label_20230808_0.6_0.4_0.5.csv', 'betaflight_3label_20230808_0.7_0.7_0.3.csv', 'radare2_3label_20230808_0.4_0.6_0.5.csv', 'mpv_3label_20230808_0.7_0.7_0.5.csv', 'radare2_3label_20230808_0.3_0.6_0.6.csv', 'framework_3label_20230808_0.6_0.7_0.7.csv', 'druid_3label_20230808_0.7_0.4_0.6.csv', 'betaflight_3label_20230808_0.6_0.6_0.7.csv', 'radare2_3label_20230808_0.3_0.7_0.5.csv', 'betaflight_3label_20230808_0.7_0.3_0.4.csv', 'betaflight_3label_20230808_0.6_0.5_0.4.csv', 'betaflight_3label_20230808_0.7_0.4_0.4.csv', 'druid_3label_20230808_0.7_0.7_0.6.csv', 'betaflight_3label_20230808_0.5_0.5_0.5.csv', 'mpv_3label_20230808_0.7_0.4_0.5.csv', 'mpv_3label_20230808_0.6_0.4_0.4.csv', 'radare2_3label_20230808_0.4_0.5_0.5.csv', 'framework_3label_20230808_0.6_0.7_0.4.csv', 'framework_3label_20230808_0.5_0.5_0.7.csv', 'mpv_3label_20230808_0.6_0.3_0.4.csv', 'druid_3label_20230808_0.6_0.5_0.4.csv', 'framework_3label_20230808_0.7_0.3_0.5.csv', 'radare2_3label_20230808_0.5_0.3_0.6.csv', 'druid_3label_20230808_0.6_0.4_0.6.csv', 'betaflight_3label_20230808_0.6_0.7_0.5.csv', 'betaflight_3label_20230808_0.7_0.4_0.7.csv', 'betaflight_3label_20230808_0.6_0.3_0.4.csv', 'radare2_3label_20230808_0.3_0.5_0.7.csv', 'betaflight_3label_20230808_0.6_0.3_0.6.csv', 'framework_3label_20230808_0.5_0.7_0.6.csv', 'mpv_3label_20230808_0.5_0.5_0.5.csv', 'mpv_3label_20230808_0.6_0.3_0.6.csv', 'radare2_3label_20230808_0.7_0.7_0.4.csv', 'framework_3label_20230808_0.6_0.7_0.5.csv', 'radare2_3label_20230808_0.7_0.3_0.4.csv', 'radare2_3label_20230808_0.7_0.6_0.6.csv', 'radare2_3label_20230808_0.6_0.3_0.6.csv', 'framework_3label_20230808_0.7_0.6_0.4.csv', 'radare2_3label_20230808_0.7_0.3_0.5.csv', 'betaflight_3label_20230808_0.5_0.6_0.5.csv', 'radare2_3label_20230808_0.5_0.3_0.5.csv', 'betaflight_3label_20230808_0.5_0.5_0.3.csv', 'framework_3label_20230808_0.6_0.6_0.5.csv', 'radare2_3label_20230808_0.7_0.7_0.5.csv', 'framework_3label_20230808_0.7_0.7_0.3.csv', 'framework_3label_20230808_0.6_0.3_0.4.csv', 'framework_3label_20230808_0.5_0.4_0.5.csv', 'framework_3label_20230808_0.5_0.5_0.4.csv', 'betaflight_3label_20230808_0.6_0.6_0.4.csv', 'mpv_3label_20230808_0.5_0.5_0.7.csv', 'betaflight_3label_20230808_0.7_0.4_0.6.csv', 'druid_3label_20230808_0.6_0.3_0.3.csv', 'framework_3label_20230808_0.7_0.6_0.3.csv', 'framework_3label_20230808_0.5_0.5_0.6.csv', 'radare2_3label_20230808_0.4_0.3_0.3.csv', 'betaflight_3label_20230808_0.6_0.3_0.3.csv', 'druid_3label_20230808_0.5_0.7_0.5.csv', 'radare2_3label_20230808_0.3_0.6_0.7.csv', 'druid_3label_20230808_0.6_0.4_0.3.csv', 'mpv_3label_20230808_0.7_0.3_0.6.csv', 'mpv_3label_20230808_0.5_0.4_0.4.csv', 'radare2_3label_20230808_0.6_0.5_0.7.csv', 'mpv_3label_20230808_0.7_0.5_0.5.csv', 'druid_3label_20230808_0.6_0.7_0.6.csv', 'druid_3label_20230808_0.6_0.3_0.7.csv', 'radare2_3label_20230808_0.3_0.6_0.4.csv', 'radare2_3label_20230808_0.6_0.6_0.3.csv', 'framework_3label_20230808_0.6_0.6_0.4.csv', 'radare2_3label_20230808_0.7_0.6_0.3.csv', 'framework_3label_20230808_0.7_0.7_0.5.csv', 'druid_3label_20230808_0.4_0.7_0.7.csv', 'druid_3label_20230808_0.5_0.3_0.3.csv', 'druid_3label_20230808_0.5_0.7_0.4.csv', 'druid_3label_20230808_0.7_0.5_0.4.csv', 'druid_3label_20230808_0.6_0.6_0.4.csv', 'betaflight_3label_20230808_0.7_0.6_0.7.csv', 'radare2_3label_20230808_0.6_0.4_0.3.csv', 'radare2_3label_20230808_0.4_0.5_0.6.csv', 'betaflight_3label_20230808_0.6_0.3_0.5.csv', 'betaflight_3label_20230808_0.5_0.5_0.7.csv', 'radare2_3label_20230808_0.7_0.4_0.4.csv', 'mpv_3label_20230808_0.7_0.5_0.3.csv', 'druid_3label_20230808_0.5_0.6_0.4.csv', 'betaflight_3label_20230808_0.7_0.5_0.4.csv', 'druid_3label_20230808_0.7_0.4_0.3.csv', 'radare2_3label_20230808_0.5_0.6_0.4.csv', 'druid_3label_20230808_0.5_0.5_0.5.csv', 'betaflight_3label_20230808_0.6_0.6_0.3.csv', 'radare2_3label_20230808_0.4_0.7_0.7.csv', 'mpv_3label_20230808_0.6_0.5_0.7.csv', 'druid_3label_20230808_0.6_0.7_0.3.csv', 'mpv_3label_20230808_0.6_0.4_0.6.csv', 'mpv_3label_20230808_0.6_0.7_0.6.csv', 'framework_3label_20230808_0.7_0.6_0.6.csv', 'radare2_3label_20230808_0.5_0.4_0.7.csv', 'framework_3label_20230808_0.7_0.6_0.7.csv', 'mpv_3label_20230808_0.6_0.3_0.3.csv', 'mpv_3label_20230808_0.5_0.6_0.4.csv', 'radare2_3label_20230808_0.6_0.6_0.7.csv', 'framework_3label_20230808_0.7_0.7_0.7.csv', 'framework_3label_20230808_0.6_0.5_0.7.csv', 'betaflight_3label_20230808_0.5_0.4_0.5.csv', 'druid_3label_20230808_0.7_0.3_0.5.csv', 'mpv_3label_20230808_0.7_0.7_0.7.csv', 'mpv_3label_20230808_0.7_0.7_0.4.csv', 'radare2_3label_20230808_0.3_0.4_0.5.csv', 'framework_3label_20230808_0.6_0.5_0.5.csv', 'mpv_3label_20230808_0.6_0.7_0.4.csv', 'framework_3label_20230808_0.5_0.4_0.6.csv', 'framework_3label_20230808_0.7_0.4_0.3.csv', 'framework_3label_20230808_0.7_0.5_0.5.csv', 'druid_3label_20230808_0.7_0.5_0.6.csv', 'betaflight_3label_20230808_0.6_0.6_0.5.csv', 'mpv_3label_20230808_0.7_0.6_0.7.csv']

    for tuple in tqdm(project_threshold_tuples):
        arr = tuple.replace(".csv", "").split("_")
        project, lifecycle_threshold, prevalence_threshold, quality_threshold = arr[0], float(arr[3]), float(arr[4]), float(arr[5])
        genealogy_path = os.path.join(config_global.GROUP_GENEALOGY_PATH, f'{project}_group_genealogy_distinct.csv')
        label_path = os.path.join(os.path.normpath(config_global.LABEL_PATH), f'20230912_{project}_3label_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}.csv')
        #if os.path.exists(label_path):
            #continue

        genealogy_df = pd.read_csv(genealogy_path)
    
        commits_log_df = Git_repo.get_commit_log_df(project)
        commits_log_df['timestamp'] = pd.to_datetime(commits_log_df['timestamp'], errors='coerce')
    
        commit_author_dict = dict(zip(commits_log_df.commit_id, commits_log_df.email))
        commit_time_dict = dict(zip(commits_log_df.commit_id, commits_log_df.timestamp))
    
        genealogy_anatomized_df = anatomize_genealogy(genealogy_df, commit_time_dict, commit_author_dict)
    
        # rank by 1rd dimension - Clone LifeCycle
        rank_by_lifecycle(genealogy_anatomized_df, is_by_genealogy, lifecycle_threshold)
    
        # rank by 2nd dimension - Clone Prevalence
        rank_by_prevalence(genealogy_anatomized_df, prevalence_threshold)
    
        # rank by 3rd dimension - Clone Bug-proneness
        buggy_commits_extractor = Buggy_commits_extractor(project)
        bugfix_commit_list = buggy_commits_extractor.identify_bugfix_commits(project)
        commits_log_df = Git_repo.get_commit_log_df(project)
        bugfix_shortcommit_list = get_intersection(commits_log_df['commit_id'], bugfix_commit_list)
        
        print(project, "has bugfix commits: ", len(bugfix_shortcommit_list))
        rank_by_bugproneness(genealogy_anatomized_df, list(set(bugfix_shortcommit_list)), quality_threshold)
    
        if is_longevous and is_prevalent and is_buggy: # decide the label by all the three dimensions
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
                lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_prevalence + row.rank_by_bugproneness) / 3),
                axis=1
            ).astype(int)
        elif is_longevous and is_buggy:
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
                lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_bugproneness) / 2),
                axis=1
            ).astype(int)
        elif is_longevous:
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df['rank_by_lifecycle']

        print("is_reusable: ", genealogy_anatomized_df['is_reusable'].value_counts())
        genealogy_anatomized_df.to_csv(label_path, index=False)


def decide_final_label_at_thresholds3(project, df, is_by_genealogy, is_longevous, is_prevalent, is_buggy):
    # rank by 3rd dimension - Clone Bug-proneness
    buggy_commits_extractor = Buggy_commits_extractor(project)
    bugfix_commit_list = buggy_commits_extractor.identify_bugfix_commits(project)
    commits_log_df = Git_repo.get_commit_log_df(project)
    bugfix_shortcommit_list = get_intersection(commits_log_df['commit_id'], bugfix_commit_list)
    print(project, "has bugfix commits: ", len(bugfix_shortcommit_list))
    for index, row in df.iterrows():
        lifecycle_threshold, prevalence_threshold, quality_threshold = float(row['t1']), float(row['t2']), float(row['t3'])
        genealogy_path = os.path.join(config_global.GROUP_GENEALOGY_PATH, f'{project}_group_genealogy_distinct.csv')
        label_path = os.path.join(os.path.normpath(config_global.LABEL_PATH), f'20230912_{project}_3label_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}.csv')
        print("label path: ", label_path)
        #if os.path.exists(label_path):
            #continue

        genealogy_df = pd.read_csv(genealogy_path)
        
        commits_log_df = Git_repo.get_commit_log_df(project)
        commits_log_df['timestamp'] = pd.to_datetime(commits_log_df['timestamp'], errors='coerce')
        
        commit_author_dict = dict(zip(commits_log_df.commit_id, commits_log_df.email))
        commit_time_dict = dict(zip(commits_log_df.commit_id, commits_log_df.timestamp))
        
        genealogy_anatomized_df = anatomize_genealogy(genealogy_df, commit_time_dict, commit_author_dict)
        
        # rank by 1rd dimension - Clone LifeCycle
        rank_by_lifecycle(genealogy_anatomized_df, is_by_genealogy, lifecycle_threshold)
        
        # rank by 2nd dimension - Clone Prevalence
        rank_by_prevalence(genealogy_anatomized_df, prevalence_threshold)
        rank_by_bugproneness(genealogy_anatomized_df, list(set(bugfix_shortcommit_list)), quality_threshold)
    
        if is_longevous and is_prevalent and is_buggy: # decide the label by all the three dimensions
            
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
                lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_prevalence + row.rank_by_bugproneness) / 3),
                axis=1
            ).astype(int)
        elif is_longevous and is_buggy:
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
                lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_bugproneness) / 2),
                axis=1
            ).astype(int)
        elif is_longevous:
            genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df['rank_by_lifecycle']

        print(genealogy_anatomized_df['is_reusable'].value_counts())
        genealogy_anatomized_df.to_csv(label_path, index=False)
    print(genealogy_anatomized_df.describe(), '\n', genealogy_anatomized_df.columns)


def extract_quality_label(project): # ,
    print("project: ", project)
    buggy_commits_extractor = Buggy_commits_extractor(project)
    # buggy_commit_list_flat = buggy_commits_extractor.identify_buggy_commits(project)
    bugfix_commit_list = buggy_commits_extractor.identify_bugfix_commits(project)
    print(project, "has bugfix commits: ", len(bugfix_commit_list))


def check(project, is_by_genealogy, is_longevous, is_prevalent, is_buggy):
    print("project: ", project)
    genealogy_path = os.path.join(config_global.GROUP_GENEALOGY_PATH, f'{project}_group_genealogy_distinct.csv')
    if not os.path.exists(genealogy_path):
        return
    
    genealogy_df = pd.read_csv(genealogy_path)

    commits_log_df = Git_repo.get_commit_log_df(project)
    commits_log_df['timestamp'] = pd.to_datetime(commits_log_df['timestamp'], errors='coerce')

    commit_author_dict = dict(zip(commits_log_df.commit_id, commits_log_df.email))
    commit_time_dict = dict(zip(commits_log_df.commit_id, commits_log_df.timestamp))

    genealogy_anatomized_df = anatomize_genealogy(genealogy_df, commit_time_dict, commit_author_dict)

    # rank by 1rd dimension - Clone LifeCycle
    rank_by_lifecycle(genealogy_anatomized_df, is_by_genealogy, config_global.lifecycle_threshold)

    # rank by 2nd dimension - Clone Prevalence
    rank_by_prevalence(genealogy_anatomized_df, config_global.prevalence_threshold)

    # rank by 3rd dimension - Clone Bug-proneness
    buggy_commits_extractor = Buggy_commits_extractor(project)
    bugfix_commit_list = buggy_commits_extractor.identify_bugfix_commits(project)
    print(project, "has bugfix commits: ", len(bugfix_commit_list))
    rank_by_bugproneness(genealogy_anatomized_df, list(set(bugfix_commit_list)), config_global.quality_threshold)
    
    genealogy_anatomized_df['is_reusable'] = genealogy_anatomized_df.apply(
            lambda row: math.floor((row.rank_by_lifecycle + row.rank_by_prevalence + row.rank_by_bugproneness) / 3),
            axis=1
        ).astype(int)
    
    label_path = os.path.join(os.path.normpath(config_global.LABEL_PATH), f'20230912_{project}_3label_{config_global.lifecycle_threshold}_{config_global.prevalence_threshold}_{config_global.quality_threshold}.csv')
    genealogy_anatomized_df.to_csv(label_path, index=False)
    return genealogy_anatomized_df




def main():
    start_time = time()

    #for project in projects:
    #    buggy_commits_extractor = Buggy_commits_extractor(project)
    #    buggy_commit_list_flat = buggy_commits_extractor.identify_bugfix_commits(project)

    #print(f'Time taken = {time() - start_time} seconds')  # print(f"Execution time: {elapsed_time:.4f} seconds")
    
    is_by_genealogy = True
    is_longevous = True
    is_prevalent = True
    is_buggy = True

    projects = list(config_global.SUBJECT_SYSTEMS_YOUNG.keys()) + list(config_global.SUBJECT_SYSTEMS_MIDDLE.keys()) + list(config_global.SUBJECT_SYSTEMS_OLD.keys()) 
    # projects = ['inav', 'FreeRDP']  #['jabref']

    '''
    with cf.ProcessPoolExecutor(max_workers=50) as executor:
         # futures = {executor.submit(extract_quality_label, project) for project in projects}
         futures = {executor.submit(decide_final_label, project, is_by_genealogy, is_longevous, is_prevalent, is_buggy) for project in projects}
         for future in tqdm(cf.as_completed(futures), desc="projects raszz-ed"):
             try:
                 buggy_commit_list_flat = future.result()
             except Exception as err:
                 print(f"Exception occurred in project: {err}")
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=['project', 'projects', 'projects_seq', 'projects_thresholds', 'projects_thresholds_seq', 'projects_thresholds_mp'], help='Run on a single project or batch projects')
    args = parser.parse_args()
    if args.role == 'project':
        for project in projects:
            print("project: ", project)
            decide_final_label(project, is_by_genealogy, is_longevous, is_prevalent, is_buggy)

    elif args.role == 'projects':
        print(f"len projects to run: {len(projects)}")
        with cf.ProcessPoolExecutor(max_workers=int(os.cpu_count() * 2 / 5)) as executor: # Understand tool doesnot support concurrent
            futures = {executor.submit(decide_final_label, project, is_by_genealogy, is_longevous, is_prevalent, is_buggy): project for project in projects} #config_global.SUBJECT_SYSTEMS_ALL.keys()}
            # executor.map(worker, projects)
            for future in tqdm(cf.as_completed(futures), total=len(futures), desc='getting Understand metrics for projects'):
                pass

    elif args.role == 'projects_seq':
        print(len(projects))
        for project in projects:
            print("project: ", project)
            decide_final_label(project, is_by_genealogy, is_longevous, is_prevalent, is_buggy)
    
    elif args.role == 'projects_thresholds':
        print(len(projects))
        with cf.ProcessPoolExecutor(max_workers=int(os.cpu_count() * 1 / 5)) as executor: # Understand tool doesnot support concurrent
            futures = {executor.submit(decide_final_label_at_thresholds, project, is_by_genealogy, is_longevous, is_prevalent, is_buggy): project for project in projects} #config_global.SUBJECT_SYSTEMS_ALL.keys()}
            # executor.map(worker, projects)
            for future in tqdm(cf.as_completed(futures), total=len(futures), desc='run all thresholds'):
                pass

        # for project in projects:
        #     print("project: ", project)
        #     decide_final_label_at_thresholds(project, is_by_genealogy, is_longevous, is_prevalent, is_buggy)

    elif args.role == 'projects_thresholds_seq':
        decide_final_label_at_thresholds2(is_by_genealogy, is_longevous, is_prevalent, is_buggy)

    elif args.role == 'projects_thresholds_mp':
        projects_all =  list(config_global.SUBJECT_SYSTEMS_YOUNG.keys()) + list(config_global.SUBJECT_SYSTEMS_MIDDLE.keys()) + list(config_global.SUBJECT_SYSTEMS_OLD.keys()) 
        
        #values = [0.3, 0.4, 0.5, 0.6, 0.7]
        #combinations = list(itertools.product(values, repeat=3))
        combinations = [(0.3, 0.3, 0.3), (0.4, 0.4, 0.4), (0.5, 0.5, 0.5), (0.6, 0.6, 0.6), (0.7, 0.7, 0.7)]
        paths = [f'20230912_{project}_3label_{comb[0]}_{comb[1]}_{comb[2]}.csv' for project in projects_all for comb in combinations]
        #print(len(paths))
        
        from glob import glob
        files = glob("/home/20cy3/topic1/clone2api/data/label/20230912_*_3label_*_*_*.csv")
        label_paths = [file.rsplit("/", 1)[1] for file in files]
        
        print(list(set(paths) - set(label_paths)))
        
        li = []
        for path in list(set(paths) - set(label_paths)):
            arr = path.replace(".csv", "").split("_")
            project, t1, t2, t3 = arr[1], arr[3], arr[4], arr[5]
            print("project-threshold: ", project, t1, t2, t3)
            li.append((project, t1, t2, t3))

        df = pd.DataFrame(li, columns=['project', 't1', 't2', 't3'])
        
        with cf.ProcessPoolExecutor(max_workers=int(os.cpu_count() * 1 / 5)) as executor: # Understand tool doesnot support concurrent
            futures = {executor.submit(decide_final_label_at_thresholds3, project, df[df['project'] == project][['t1', 't2', 't3']], is_by_genealogy, is_longevous, is_prevalent, is_buggy): project for project in df['project'].unique()} #config_global.SUBJECT_SYSTEMS_ALL.keys()}
            # executor.map(worker, projects)
            for future in tqdm(cf.as_completed(futures), total=len(futures), desc='run all thresholds3'):
                pass


    '''
    for project in projects:
        print(f"project: {project}")

        buggy_commits_extractor = Buggy_commits_extractor(project)
        buggy_commit_list_flat = buggy_commits_extractor.identify_buggy_commits(project)
    

    print("len projects: ", len(config_global.SUBJECT_SYSTEMS_ALL.keys()))
    print("projects: ", config_global.SUBJECT_SYSTEMS_ALL.keys())
    with cf.ProcessPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(extract_quality_label, project): project for project in config_global.SUBJECT_SYSTEMS_ALL.keys()} #config_global.SUBJECT_SYSTEMS_ALL.keys()}
        for future in tqdm(cf.as_completed(futures), total=len(futures), desc='getting bug inducing commits by raszz for projects'):
            pass
    '''
    
    
if __name__ == '__main__':
    main()
    #projects = list(config_global.SUBJECT_SYSTEMS_YOUNG.keys()) + list(config_global.SUBJECT_SYSTEMS_MIDDLE.keys()) + list(config_global.SUBJECT_SYSTEMS_OLD.keys()) 
    #for project in projects:
    #    check(project, True, True, True, True)
        
    
