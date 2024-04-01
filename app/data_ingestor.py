import os
import json
import csv

class DataIngestor:
    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path
        self.data = self.read_csv(csv_path)

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]

    def read_csv(self, csv_path: str):
        # Open the CSV file in read mode
        with open(csv_path, 'r') as file:
            csvReader = csv.reader(file)
            # Skip the header
            next(csvReader)

            # read line by line
            data = []

            for values in csvReader:
                # create a dictionary from the values
                data.append({
                    "YearStart": values[1],
                    "YearEnd": values[2],
                    "LocationAbbr": values[3],
                    "LocationDesc": values[4],
                    "Datasource": values[5],
                    "Class": values[6],
                    "Topic": values[7],
                    "Question": values[8],
                    "Data_Value_Unit": values[9],
                    "Data_Value_Type": values[10],
                    "Data_Value": values[11],
                    "Data_Value_Alt": values[12],
                    "Data_Value_Footnote_Symbol": values[13],
                    "Data_Value_Footnote": values[14],
                    "Low_Confidence_Limit": values[15],
                    "High_Confidence_Limit": values[16],
                    "Sample_Size": values[17],
                    "Total": values[18],
                    "Age(years)": values[19],
                    "Education": values[20],
                    "Gender": values[21],
                    "Income": values[22],
                    "Race/Ethnicity": values[23],
                    "GeoLocation": values[24],
                    "ClassID": values[25],
                    "TopicID": values[26],
                    "QuestionID": values[27],
                    "DataValueTypeID": values[28],
                    "LocationID": values[29],
                    "StratificationCategory1": values[30],
                    "Stratification1": values[31],
                    "StratificationCategoryId1": values[32],
                    "StratificationID1": values[33]
                })
        
        # Return the list of dictionaries containing the data
        return data
    
    def process_question(self, req_data, request_type):
        # For each request type
        question = req_data['question']
        
        if request_type == 'states_mean_request':
            return self.states_mean_request(question)
        elif request_type == 'state_mean_request':
            return self.state_mean_request(question, req_data['state'])
        elif request_type == 'best5_request':
            return self.best5_request(question)
        elif request_type == 'worst5_request':
            return self.worst5_request(question)
        elif request_type == 'global_mean_request':
            return self.global_mean_request(question)
        elif request_type == 'diff_from_mean_request':
            return self.diff_from_mean_request(question)
        elif request_type == 'state_diff_from_mean_request':
            return self.state_diff_from_mean_request(question)
        elif request_type == 'mean_by_category_request':
            return self.mean_by_category_request(question)
        elif request_type == 'state_mean_by_category_request':
            return self.state_mean_by_category_request(question)

    def states_mean_request(self, question: str):
        # Get all data corresponding to the question
        data_valid = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get all states
        states = list(set([d['LocationDesc'] for d in data_valid]))

        # Get the mean for each state
        mean_by_state = {}
        mean_by_state_divisor = {}
        # Go through all the data
        for d in data_valid:
            state = d['LocationDesc']
            mean = float(d['Data_Value'])
            if state not in mean_by_state:
                mean_by_state[state] = 0
                mean_by_state_divisor[state] = 0
            mean_by_state[state] += mean
            mean_by_state_divisor[state] += 1

        for state in states:
            mean_by_state[state] /= mean_by_state_divisor[state]

        return mean_by_state
    
    def state_mean_request(self, question: str, state: str):
        # Get all data corresponding to the question
        data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the mean for the state
        mean = sum([float(d['Data_Value']) for d in data]) / len(data)
        
        return mean
    
    def best5_request(self, question: str):
        # Get all data corresponding to the question
        data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the best 5 states
        best5 = sorted(data, key=lambda x: x['Data_Value'])[:5]
        
        return best5
    
    def worst5_request(self, question: str):
        # Get all data corresponding to the question
        data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the worst 5 states
        worst5 = sorted(data, key=lambda x: x['Data_Value'])[-5:]
        
        return worst5
    
    def global_mean_request(self, question: str):
        # Get all data corresponding to the question
        data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the global mean
        mean = sum([float(d['Data_Value']) for d in data]) / len(data)
        
        return mean
    
    def diff_from_mean_request(self, question: str):
        # Get the global mean
        global_mean = self.global_mean_request(question)

        # Get all data corresponding to the question
        data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the difference from the global mean for each state
        diff_by_state = {}
        for d in data:
            state = d['LocationDesc']
            mean = float(d['Data_Value'])
            diff_by_state[state] = mean - global_mean
        
        return diff_by_state
    
    def state_diff_from_mean_request(self, question: str, state: str):
        # Get the global mean
        global_mean = self.global_mean_request(question)
        
        # Get all data corresponding to the question and state
        data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the state mean
        state_mean = sum([float(d['Data_Value']) for d in data]) / len(data)

        # Get the difference from the global mean for the state
        diff = state_mean - global_mean

        return diff
    
    def mean_by_category_request(self, question: str, category: str):   
        # Get all data corresponding to the question
        data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get all categories
        categories = list(set([d['StratificationCategory1'] for d in data]))

        # Get the mean for each category
        mean_by_category = {}
        mean_by_category_divisor = {}

        for category in categories:
            category_data = [d for d in data if d['StratificationCategory1'] == category]
            mean = sum([float(d['Data_Value']) for d in category_data]) / len(category_data)
            mean_by_category[category] += mean
            mean_by_category_divisor[category] += 1

        for category in categories:
            mean_by_category[category] /= mean_by_category_divisor[category]

        return mean_by_category
    
    def state_mean_by_category_request(self, question: str, state: str, category: str):
        # Get all data corresponding to the question and state
        data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get all categories
        categories = list(set([d['StratificationCategory1'] for d in data]))

        # Get the mean for each category
        mean_by_category = {}
        mean_by_category_divisor = {}

        for category in categories:
            category_data = [d for d in data if d['StratificationCategory1'] == category]
            mean = sum([float(d['Data_Value']) for d in category_data]) / len(category_data)
            mean_by_category[category] += mean
            mean_by_category_divisor[category] += 1

        for category in categories:
            mean_by_category[category] /= mean_by_category_divisor[category]

        return mean_by_category