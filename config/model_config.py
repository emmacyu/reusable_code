import numpy as np
import os
from config import config_global

# parameter grids
PARAM_GRID_LR = {
    'penalty': ['l1', 'l2', 'elasticnet', 'none'],
    'C' : np.logspace(-4, 8, 15),
    'solver': ['lbfgs','newton-cg','liblinear','sag','saga'],
    'max_iter': [100, 1000,2500, 5000]
}

PARAM_GRID_DT = {
    'splitter': ["best", "random"],
    'criterion': ["gini", "entropy"],
    'min_samples_split': [2, 3, 4, 5]
}

PARAM_GRID_RF = {
 #'max_depth': [5, 11, 12,15],
 'criterion':['entropy','gini'],
 #'max_samples': [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
 #'class_weight':['balanced_subsample'],
 #'max_features': [3, 5, 15, 20, 23, 25, 30], # ['auto', 'sqrt'],
 #'min_samples_leaf': [1, 2, 4],
 'min_samples_split': [2, 5, 10],
 'n_estimators': [100]#, 300]#, 600] # , 1000, 1200, 1400]
}

PARAM_GRID_XGB = {
    #'kernel': ['linear', 'rbf'],
    #'min_child_weight': [4, 5, 10, 15],  #[1, 5, 10, 15], # 1, 5,
    #'gamma': [0.14], # [ 0.13, 0.135, 0.14,0.145, 0.15, 0.16,  0.17, 0.18, 0.2], # [ 0.05, 0.1, 0.2, 0.4, 0.5, 0.8, 1, 1.5, 1.6,2,3.2, 5, 6.4, 12.8, 25.6,51.2,102.4, 200], # 0.01, 0.05,
    #'subsample': [ 0.9, 0.95], #0.6, 0.7, 0.8,
    #'colsample_bytree': [ 0.8, 0.9, 1.0, 1.1, 2, 3 ],
    #'max_depth':[7, 11, 13, 14], # [7, 11, 13, 14], # range(1, 15, 1), #3, 4, 5, 6,7,8,9,2,
    #'min_child_weight': range(3, 6, 1), # [4], #
    #'learning_rate': [0.1, 0.15, 0.2, 0.25, 0.6, 0.7],  #, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7], # 0.01, 0.03,
    'n_estimators': range(350, 500, 50) #[200, 300,500, 600],# 65,80,100,115,130,150,
    ,'min_samples_leaf': range(1, 10, 1)
    #'reg_alpha': [0.4, 0.6, 0.7, 0.8],  # 0.1,0.2,0.4,0.8,1.6,3.2,6.4,12.8,25.6,51.2,102.4,200],
    #'reg_lambda': [0.8], # [0.7, 0.8, 0.9] #,3.2,6.4,12.8,25.6,51.2,102.4,200, 0,0.1,0.2,0.4,0.5, 0.6,1.4, 1.0, 1.1, 1.2, 1.3, 1.5, 1.6]
}

PARAM_GRID_CAT = {#'depth': range(5, 15, 1) #[4,5,6,7,8,9, 10],
            #'learning_rate' : [0.01,0.02,0.03,0.04, 0,1, 0.16],
            #'iterations'  : [10, 20,30,40,50,60,70,80,90, 100],
            'loss_function': ['Logloss', 'CrossEntropy']
            #,'l2_leaf_reg': [3,1,5,10,100] # np.logspace(-20, -19, 3) #,
            ,'bootstrap_type': ['Bernoulli', 'Bayesian', 'Poisson', 'No']
            ,'n_estimators': range(350, 500, 50)
            }

PARAM_GRID_LGBM = {#'depth': range(5, 15, 1) #[4,5,6,7,8,9, 10],
            #'learning_rate' : [0.01,0.02,0.03,0.04, 0,1, 0.16],
            #'iterations'  : [10, 20,30,40,50,60,70,80,90, 100],
            'loss_function': ['Logloss', 'CrossEntropy']
            #,'l2_leaf_reg': [3,1,5,10,100] # np.logspace(-20, -19, 3) #,
            ,'bootstrap_type': ['Bernoulli', 'Bayesian', 'Poisson', 'No']
            ,'n_estimators': range(350, 500, 50)
            }

PARAM_GRID_ADA = {
            'n_estimators': range(50, 500, 50)
            }

PARAM_GRID_SVC = {
            #'C': [0.1, 1, 10], # 'C': [0.1, 1, 10, 100, 1000],
            #'gamma': [0.01, 0.001], #'gamma': [1, 0.1, 0.01, 0.001, 0.0001],
            'kernel': ['linear'] #'kernel': ['rbf', 'linear']
}

MODEL_RESULTS_RF = {
'XChange': os.path.join(config_global.MODEL_PATH, "XChange_RandomForestClassifier.pkl"),
'xp': os.path.join(config_global.MODEL_PATH, "xp_RandomForestClassifier.pkl"),
'zaproxy': os.path.join(config_global.MODEL_PATH, "zaproxy_RandomForestClassifier.pkl")
}

MODEL_RESULTS = {
    'Anki-Android': os.path.join(config_global.MODEL_PATH, "Anki-Android_CatBoostClassifier.pkl"),
    'che': os.path.join(config_global.MODEL_PATH, "che_RandomForestClassifier_0.8172.pkl"),
    'checkstyle': os.path.join(config_global.MODEL_PATH, "checkstyle_AdaBoostClassifier_0.8810.pkl"),
    'druid': os.path.join(config_global.MODEL_PATH, "druid_LGBMClassifier_0.7402.pkl"),
    'framework': os.path.join(config_global.MODEL_PATH, "framework_RandomForestClassifier_0.7375.pkl"),
    'gatk': os.path.join(config_global.MODEL_PATH, "gatk_CatBoostClassifier_0.75.pkl"),
    'graylog2-server': os.path.join(config_global.MODEL_PATH, "graylog2-server_CatBoostClassifier_0.712820513.pkl"),
    'grpc-java': os.path.join(config_global.MODEL_PATH, "grpc-java_LGBMClassifier_0.7664.pkl"),
     #'hazelcast': os.path.join(config_global.MODEL_PATH, "Anki-Android_CatBoostClassifier.pkl"),
    'jabref': os.path.join(config_global.MODEL_PATH, "jabref_RandomForestClassifier_0.7022.pkl"),
    #'k': os.path.join(config_global.MODEL_PATH, "k_CatBoostClassifier.pkl"),
    'k-9': os.path.join(config_global.MODEL_PATH, "k-9_CatBoostClassifier.pkl"),
    #'mage': os.path.join(config_global.MODEL_PATH, "Anki-Android_CatBoostClassifier.pkl"),
    'MinecraftForge': os.path.join(config_global.MODEL_PATH, "minecraftForge_RandomForestClassifier_0.7536.pkl"),
    'molgenis': os.path.join(config_global.MODEL_PATH, "molgenis_CatBoostClassifier.pkl"),
    'muikku': os.path.join(config_global.MODEL_PATH, "muikku_CatBoostClassifier.pkl"),
    #'nd4j': os.path.join(config_global.MODEL_PATH, "Anki-Android_CatBoostClassifier.pkl"),
    'netty': os.path.join(config_global.MODEL_PATH, "netty_LGBMClassifier_0.7205.pkl"),
    'openhab1-addons': os.path.join(config_global.MODEL_PATH, "openhab1-addons_CatBoostClassifier.pkl"),
    'pinpoint': os.path.join(config_global.MODEL_PATH, "pinpoint_CatBoostClassifier.pkl"),
    'presto': os.path.join(config_global.MODEL_PATH, "presto_CatBoostClassifier_0.687719.pkl"),
    'product-apim': os.path.join(config_global.MODEL_PATH, "product-apim_LGBMClassifier_0.8810.pkl"),
    'realm-java': os.path.join(config_global.MODEL_PATH, "realm-java_CatBoostClassifier.pkl"),
    'reddeer': os.path.join(config_global.MODEL_PATH, "reddeer_CatBoostClassifier.pkl"),
    'RxJava': os.path.join(config_global.MODEL_PATH, "RxJava_CatBoostClassifier.pkl"),
    'smarthome': os.path.join(config_global.MODEL_PATH, "smarthome_AdaBoostClassifier_0.7183.pkl"),
    'Terasology': os.path.join(config_global.MODEL_PATH, "Terasology_CatBoostClassifier.pkl"),
    #'wildfly-camel': os.path.join(config_global.MODEL_PATH, "wildfly-camel_CatBoostClassifier.pkl"),
    'XChange': os.path.join(config_global.MODEL_PATH, "XChange_CatBoostClassifier_0.741135.pkl"),
    'xp': os.path.join(config_global.MODEL_PATH, "xp_CatBoostClassifier.pkl"),
    'zaproxy': os.path.join(config_global.MODEL_PATH, "zaproxy_CatBoostClassifier.pkl")
    #'ant': os.path.join(config_global.MODEL_PATH, "Anki-Android_CatBoostClassifier.pkl"),
    #'spring-boot': os.path.join(config_global.MODEL_PATH, "Anki-Android_CatBoostClassifier.pkl")
}


