import os, multiprocessing, pickle, json, sys, gc, argparse, itertools
sys.path.append(".")
sys.path.append("..")
from multiprocessing import Pool
from glob import glob
from config import config_global, model_config
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.model_selection import RandomizedSearchCV, GridSearchCV, RepeatedKFold, RepeatedStratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.metrics import roc_auc_score
from tqdm import tqdm
from sklearn.metrics import make_scorer, roc_auc_score, accuracy_score, f1_score, recall_score, precision_score
import concurrent.futures as cf
from xgboost import XGBClassifier # gpu in conda
# from xgboost import xgb 
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier
from imblearn.over_sampling import SMOTE #,ADASYN
from collections import Counter
import numpy as np


def load_data(project, lifecycle_threshold, prevalence_threshold, quality_threshold):
    # load project data
    # reusable_clone_path = os.path.join(config_global.DATASET_PATH, "%s_raw_dataset.csv" % project)
    reusable_clone_path = os.path.join(config_global.DATASET_PATH, f'20230912_{project}_raw_dataset_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}.csv')
        
    reusable_clone_df = pd.read_csv(reusable_clone_path)
    reusable_clone_df = reusable_clone_df[config_global.FEATURES]
    print(project, " dataset: ", reusable_clone_df.shape)
    x = reusable_clone_df.drop('is_reusable', axis=1)
    y = reusable_clone_df['is_reusable']

    return x, y


def load_data_cross(projects, lifecycle_threshold, prevalence_threshold, quality_threshold):
    # load project data
    # reusable_clone_path = os.path.join(config_global.DATASET_PATH, "%s_raw_dataset.csv" % project)
    projects_df_list = []
    for project in projects:
        reusable_clone_path = os.path.join(config_global.DATASET_PATH, f'20230912_{project}_raw_dataset_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}.csv')
        reusable_clone_df = pd.read_csv(reusable_clone_path)
        projects_df_list.append(reusable_clone_df)
    reusable_clone_df = pd.concat(projects_df_list, ignore_index=True)[config_global.FEATURES]
    
    x = reusable_clone_df.drop('is_reusable', axis=1)
    y = reusable_clone_df['is_reusable']

    return x, y.to_numpy()


def grid_search(x_train, y_train, model, model_paras):
    #rcv = RepeatedKFold(n_splits=10, n_repeats=10, random_state=1) # 10 * 10 repeated k-fold cv
    rcv = RepeatedStratifiedKFold(n_splits=10, n_repeats=10, random_state=1)  # 10 * 10 repeated k-fold cv

    # Multiple metrics can be specified in a dictionary
    scoring = {
        'AUC': 'roc_auc',
        'Accuracy': make_scorer(accuracy_score),
        'F1': 'f1',
        'Recall': 'recall',
        'Precision': 'precision'
    }

    grid_search = GridSearchCV(estimator=model #  #xgb0
                               , param_grid=model_paras   # PARAM_GRID_XGB
                               , cv=rcv  #, cv=10
                               , scoring=scoring 
                               # , scoring='roc_auc'
                               #,return_train_score=True
                               # , refit=True
                               , refit='AUC'
                               , n_jobs=-1
                               , verbose = 0
                               )

    grid_search.fit(x_train.values, y_train)

    # directly used the returned best_estimator model from cross-validation to predict testing datasets with roc_auc_score provided by cross-validation
    best_model = grid_search.best_estimator_
    
    # Access the different evaluation metrics for the best model
    results = grid_search.cv_results_
    # print("Mean Test Accuracy:", results['mean_test_Accuracy'][grid_search.best_index_])
    # print("Mean Test F1:", results['mean_test_F1'][grid_search.best_index_])
    # print("Mean Test Recall:", results['mean_test_Recall'][grid_search.best_index_])
    # print("Mean Test Precision:", results['mean_test_Precision'][grid_search.best_index_])

    return grid_search, best_model


def fine_tune(project, model, model_paras, lifecycle_threshold, prevalence_threshold, quality_threshold):
    try:
        sm = SMOTE(sampling_strategy='auto', random_state=42)
        x, y = load_data(project, lifecycle_threshold, prevalence_threshold, quality_threshold)
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
        # counter1 = Counter(y_train)

        if y_train.shape[0] == 0 or y_test.shape[0] == 0:
            return classifier, None
        
        x_train, y_train = sm.fit_resample(x_train, y_train)
        #counter2 = Counter(y_train_sm)
        grid_search_model, best_model = grid_search(x_train, y_train, model, model_paras)
    
        auc_score_refit = best_model.score(x_test, y_test)
        #auc_score_refit = grid_search_model.score(x_test, y_test)
        #auc_score_sklearn = roc_auc_score(y_test, grid_search_model.predict(x_test))
        # save auc
        classifier = type(model).__name__
        print(project, classifier, auc_score_refit)
    
        # save checkpoint testing file
        tuple_objects = (grid_search_model, best_model, x_train, y_train, x_test, y_test, auc_score_refit)
        tuple_objects_path = os.path.join(config_global.MODEL_PATH, f"20230912_{project}_{classifier}_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}_{auc_score_refit}.pkl")
        with open(tuple_objects_path, 'wb') as fp:
            pickle.dump(tuple_objects, fp)
    
        return classifier, auc_score_refit
        # load tuple
        # (grid_search_model, x_train, y_train, x_test, y_test, auc_score) = pickle.load(open("tuple_model.pkl", 'rb'))
    except Exception as err:
        print(f"failed {project}-{err}")


def process_project_classifier_pair(project_classifier_pair):
    project, classifier = project_classifier_pair
    print(project, classifier)
    try:
        if classifier == 'DecisionTreeClassifier':
            model = DecisionTreeClassifier(random_state=0)
        elif classifier == 'RandomForestClassifier':
            model = RandomForestClassifier(oob_score=True, random_state=42)
        elif classifier == 'XGBClassifier':
            # model = XGBClassifier(tree_method='gpu_hist', max_bin=128, gpu_id=1, n_iter_no_change=5) 
            model = XGBClassifier(n_iter_no_change=5) 
        elif classifier == 'CatBoostClassifier':
            model = CatBoostClassifier(task_type='CPU')
        elif classifier == 'LGBMClassifier':
            model = LGBMClassifier(objective='binary', random_state=5) #, device_type='gpu')
        elif classifier == 'AdaBoostClassifier':
            model = AdaBoostClassifier(random_state=0)
        elif classifier == 'SVC':
            model = svm.SVC(random_state=42)

        model_paras = model_config.model_dict[classifier]
        # classifier, auc_score_refit = fine_tune(project, model, model_paras, config_global.threshold)
        classifier, auc_score_refit = fine_tune(project, model, model_paras, lifecycle_threshold, prevalence_threshold, quality_threshold)
        return (project, classifier, auc_score_refit)
    except Exception as err:
        print(f"failed:  {project}: {err}")
        return None
    
def process_project_threshold_classifier_pair(project_threshold_classifier_pair):
    project, classifier, lifecycle_threshold, prevalence_threshold, quality_threshold = project_threshold_classifier_pair
    print(project, classifier)
    try:
        if classifier == 'DecisionTreeClassifier':
            model = DecisionTreeClassifier(random_state=0)
        elif classifier == 'RandomForestClassifier':
            model = RandomForestClassifier(oob_score=True, random_state=42)
        elif classifier == 'XGBClassifier':
            # model = XGBClassifier(tree_method='gpu_hist', max_bin=128, gpu_id=1, n_iter_no_change=5) 
            model = XGBClassifier(n_iter_no_change=5) 
        elif classifier == 'CatBoostClassifier':
            model = CatBoostClassifier(task_type='CPU')
        elif classifier == 'LGBMClassifier':
            model = LGBMClassifier(objective='binary', random_state=5) #, device_type='gpu')
        elif classifier == 'AdaBoostClassifier':
            model = AdaBoostClassifier(random_state=0)
        elif classifier == 'SVC':
            model = svm.SVC(random_state=42)

        model_paras = model_config.model_dict[classifier]
        # classifier, auc_score_refit = fine_tune(project, model, model_paras, config_global.threshold)
        classifier, auc_score_refit = fine_tune(project, model, model_paras, lifecycle_threshold, prevalence_threshold, quality_threshold)
        return (project, classifier, auc_score_refit)
    except Exception as err:
        print(f"failed:  {project}: {err}")
        return None
    

def get_performance_within_projects():
    # values = [0.3, 0.4, 0.5, 0.6, 0.7]
    # combinations = list(itertools.product(values, repeat=3))
    # combinations = [(0.3, 0.3, 0.3), (0.4, 0.4, 0.4), (0.6, 0.6, 0.6), (0.7, 0.7, 0.7)]
    combinations = [(0.6, 0.6, 0.6)]
    for combo in combinations:
        print("combo: ", combo)
        lifecycle_threshold, prevalence_threshold, quality_threshold = combo[0], combo[1], combo[2]

        dataset_files = glob(f"{config_global.DATASET_PATH}/20230912_*_raw_dataset_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}.csv")
        # dataset_files = glob('/home/20cy3/topic1/clone2api/data/dataset/*_raw_dataset_20230808_0.5.csv')
        print(len(dataset_files))
        projects = set([os.path.basename(file).split("_")[1] for file in dataset_files])
        # projects = list(config_global.SUBJECT_SYSTEMS_MIDDLE.keys()) + list(config_global.SUBJECT_SYSTEMS_OLD.keys())
        projects = ['spock', 'PocketHub']
        print("# projects: ", len(projects))
    
        #project_classifier_threshold_tuples = [(project, classifier, lifecycle_threshold, prevalence_threshold, quality_threshold) for project in projects for classifier in list(model_config.model_dict.keys()) 
        #                            if len(glob(f"{config_global.MODEL_PATH}/20230912_{project}_{classifier}_{lifecycle_threshold}_{prevalence_threshold}_{quality_threshold}_*"))==0]
        project_classifier_threshold_tuples = [(project, classifier, lifecycle_threshold, prevalence_threshold, quality_threshold) for project in projects for classifier in list(model_config.model_dict.keys())]
        
        # dateset_files = glob('/home/20cy3/topic1/clone2api/data/dataset/*_raw_dataset_20230808_*_*_*.csv')
        # project_threshold_files = [os.path.basename(file).replace(".csv", "").split("_") for file in dateset_files]
        # project_threshold_tuples = [(file[0], file[4], file[5], file[6]) for file in project_threshold_files]
        # project_threshold_classifier_pairs = [(tuple[0], tuple[1], tuple[2], tuple[3], classifier) 
        #                                       for tuple in project_threshold_tuples for classifier in list(model_config.model_dict.keys()) 
        #                             if len(glob(f"{config_global.MODEL_PATH}/20230824_{tuple[0]}_{classifier}_{tuple[1]}_{tuple[2]}_{tuple[3]}_*"))==0]
        
        with cf.ProcessPoolExecutor(max_workers=10) as executor:
            # results = list(tqdm(executor.map(process_project_classifier_pair, project_classifier_threshold_tuples), total=len(project_classifier_threshold_tuples)))
            results = list(tqdm(executor.map(process_project_threshold_classifier_pair, project_classifier_threshold_tuples), total=len(project_classifier_threshold_tuples)))
        perf_df = pd.DataFrame(columns=["Project", "Classifier", "AUC_Score_Refit"])
        for result in results:
            if result is not None:
                project, classifier, auc_score_refit = result
                perf_df.loc[len(perf_df)] = [project, classifier, auc_score_refit]
        perf_df.to_csv('result_AUC.csv')
    
        # for project_classifier_pair in project_classifier_pairs:
        #     process_project_classifier_pair(project_classifier_pair)

    
    gc.collect()


def get_performance_cross_projects():
    for classifier in model_config.model_dict.keys():
        try:
            if classifier == 'DecisionTreeClassifier':
                model = DecisionTreeClassifier(random_state=0)
            elif classifier == 'RandomForestClassifier':
                model = RandomForestClassifier(oob_score=True, random_state=42)
            elif classifier == 'XGBClassifier':
                # model = XGBClassifier(tree_method='gpu_hist', max_bin=128, gpu_id=1, n_iter_no_change=5) 
                model = XGBClassifier(n_iter_no_change=5) 
            elif classifier == 'CatBoostClassifier':
                model = CatBoostClassifier(task_type='CPU')
            elif classifier == 'LGBMClassifier':
                model = LGBMClassifier(objective='binary', random_state=5) #, device_type='gpu')
            elif classifier == 'AdaBoostClassifier':
                model = AdaBoostClassifier(random_state=0)
            elif classifier == 'SVC':
                model = svm.SVC(random_state=42)
            elif classifier == 'NaiveBayes':
                model = GaussianNB()
            else:
                return
    
            model_paras = model_config.model_dict[classifier]
    
            train_projects = list(config_global.SUBJECT_SYSTEMS_MIDDLE.keys()) + list(config_global.SUBJECT_SYSTEMS_OLD.keys())
            test_projects = list(config_global.SUBJECT_SYSTEMS_YOUNG.keys())
            x_train, y_train = load_data_cross(train_projects, config_global.threshold)
            x_test, y_test = load_data_cross(test_projects, config_global.threshold)
        
            print(x_train.shape, y_train.shape, x_test.shape, y_test.shape)
            print("Unique values in ground truth:", np.unique(y_test))
            
            if y_train.shape[0] == 0 or y_test.shape[0] == 0:
                return classifier, None
            
            sm = SMOTE(sampling_strategy='auto', random_state=42)
            x_train, y_train = sm.fit_resample(x_train, y_train)
            #counter2 = Counter(y_train_sm)
            grid_search_model, best_model = grid_search(x_train, y_train, model, model_paras)
            print("hello")
            print(x_train.head(5))
            print(x_test.head(5))
            #auc_score = best_model.score(x_test, y_test)
            auc_score_refit = best_model.score(x_test, y_test)
            
            #auc_score_sklearn = roc_auc_score(y_test, grid_search_model.predict(x_test))
            classifier = type(model).__name__
        
            # save checkpoint testing file
            tuple_objects = (grid_search_model, best_model, x_train, y_train, x_test, y_test, auc_score_refit)
            print("hello1")
            tuple_objects_path = os.path.join(config_global.MODEL_PATH, "20230912_cross_%s_%s_%s.pkl" % (classifier, config_global.threshold, auc_score_refit))
            print("hello2")
            with open(tuple_objects_path, 'wb') as fp:
                print("hello3")
                pickle.dump(tuple_objects, fp)
        
        except Exception as err:
            print(f"failed cross: {err}")


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=['within', 'cross'], help='Run within-project prediction or cross-project prediction')
    args = parser.parse_args()
    if args.role == 'within':
        get_performance_within_projects()

    elif args.role == 'cross':
        get_performance_cross_projects()
