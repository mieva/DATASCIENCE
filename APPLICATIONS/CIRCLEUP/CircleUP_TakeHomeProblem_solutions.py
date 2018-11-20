# Functions to answer the CircleUP take-home problem's questions.

# Import libraries
import numpy as np
import pandas as pd
from scipy import stats

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
import itertools
from sklearn.metrics import classification_report,confusion_matrix
from sklearn import metrics


###########################################################
# QUESTION 1, point 1: 
###########################################################
def UsersByContent(file):
	# Function that calculate the total number of contents created by esch user

	# Import dataset in Pandas data frame
	message_data = pd.read_csv(file)
	#message_data.head()

	# Sum over content_count by user
	df_by_user = message_data.groupby('user_id').sum()

	# Select and Sort
	df = df_by_user[(df_by_user['content_count']>500)]
	dfsort = df.sort_values(by='content_count', ascending=False)

	print(" ***** Users with more than 500 contents created are: *****")
	print(dfsort[['content_count']])
	print("*****************************************")

#########################################################
# QUESTION 1, point 2:  
#########################################################

def linreg(row):
	# Define a function to calculate the slope of the regression line that is our metric = 'growing rate'

    slope, intercept, r_value, p_value, std_err = stats.linregress(row['days_since'], row['total_engagement'])
    return slope


def FindFastetGrowingUsers(file):
	# Function to find the 10 best users based on the growing rate.
	# Growing rate metric is defined as the slope of the regression 
	# line between the number of positive engaments and the days when this number has been obtained. 

	# Import dataset in Pandas data frame
	message_data = pd.read_csv(file)

	# Convert strings of date in a date object
	message_data['content_created_date'] = pd.to_datetime(message_data['content_created_date'], format="%m/%d/%Y")

	# Select only needed variables
	df = message_data[['user_id', 'content_created_date', 'total_engagement']]

	# convert date in day_since to be able to apply regression
	df['days_since'] = (df.content_created_date - pd.to_datetime('2015-01-01') ).astype('timedelta64[D]')

	# reshape df to have a list of 'days_since' for each user
	dfreshaped_x = df.groupby('user_id')['days_since'].apply(list)

	# reshape df to have a list of 'total_engagement' for each user
	dfreshaped_y = df.groupby('user_id')['total_engagement'].apply(list)

	# Reshape data to have it in a useful format
	data = {'days_since':dfreshaped_x, 'total_engagement':dfreshaped_y}
	day_eng_list_df = pd.DataFrame(data)

	# sanity check on the lenght of the lists in order to have a meaning regression points
	day_eng_list_df['counts'] = day_eng_list_df['days_since'].apply(len)

	# Remove users with less than 10 entrie per list of days_since and total_engagement
	cut_df = day_eng_list_df[day_eng_list_df['counts']>10]
	
    # Apply linreg function to cut_df data frame 
	cut_df['growing_rate'] = cut_df.apply(linreg, axis=1)

	# Sort and select first 10 users
	sorted_df = cut_df.sort_values('growing_rate', ascending=False)
	first_users = sorted_df[:10]

	print(" ***** Fastet growing users are: *****")
	print(first_users[['growing_rate']])
	print(" **************************************")


################################################################
# QUESTION 2: LEARNING MODEL
################################################################


def BuildAndRunLearningModel(file2,file3,file4):
	# Function that build a run a Support Vector Machine model to predict the outcome 'response', using 
	# user features

	# import data
	features = pd.read_csv(file2)
	users = pd.read_csv(file3)
	test = pd.read_csv(file4)

	# put togheter to have the final dataset
	train = users.set_index('user_id').join(features.set_index('user_id'))

	# Check for Missing Data
	missed_data = train.isnull().sum().sum()
	print("missed data are: ", missed_data)

	# Check if data sample is balanced
	balance = train['response'].value_counts()
	print("Sample composition is: ")
	print(balance)

	# After EDA, (see report please) we decide to use only a subset of features (those non correlated) to train the 
	# learning model.
	# Subsetting the train dataset
	trainmod = train[['response','var_1','var_2','var_5','var_7','var_12']]

	# Split the dataset in train and test
	X_train, X_test, y_train, y_test = train_test_split(trainmod.drop('response',axis=1), 
                                                    trainmod['response'], test_size=0.30, 
                                                    random_state=101)
	# Features rescaling
	scaler = StandardScaler()
	scaler.fit(X_train)
	X_train=scaler.transform(X_train)
	X_test=scaler.transform(X_test)

	# Buiding the classifier
	svc_model = SVC(class_weight='balanced')

	# Model fit
	svc_model.fit(X_train,y_train)

	# Model predictions
	svc_pred = svc_model.predict(X_test)

	# Compute and print confusion matrix
	cm = confusion_matrix(y_test,svc_pred)
	# Normalization
	cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
	print("Normalized Confusion Matrix is: ")
	print(cm)

	# Print classification report
	print(classification_report(y_test,svc_pred))

	# Classification applied to model_test_file
	# Drop misbehaving features
	testmod = test.drop(['var_3', 'var_4','var_6','var_8','var_9','var_10','var_11'],axis=1)

	# Set user_id as index
	testmod = testmod.set_index('user_id') 

	# Reshaping dataset to be used as input to the model
	X_test_newsample = testmod.as_matrix()

	# Rescaling
	X_test_newsample = scaler.transform(X_test_newsample)

	# apply SVM
	svc_new_pred = svc_model.predict(X_test_newsample)

	# see the results taking a look at the predictions created
	testmod['new_pred'] = svc_new_pred

	# Print results
	print(" ***** Predicted response values for new users are: *****")
	print(testmod[['new_pred']])
	print(" **************************************")


######################################################################

def main():

	# Files paths:
	file1='./user_message.csv'
	file2='./user_features.csv'
	file3='./user.csv'
	file4='./model_test_file.csv'

	UsersByContent(file1)
	FindFastetGrowingUsers(file1)
	BuildAndRunLearningModel(file2,file3,file4)


if __name__== "__main__":
	main()


