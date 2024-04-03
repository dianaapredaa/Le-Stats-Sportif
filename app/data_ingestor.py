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
            return self.best5_request(question, )
        elif request_type == 'worst5_request':
            return self.worst5_request(question)
        elif request_type == 'global_mean_request':
            return self.global_mean_request(question)
        elif request_type == 'diff_from_mean_request':
            return self.diff_from_mean_request(question)
        elif request_type == 'state_diff_from_mean_request':
            return self.state_diff_from_mean_request(question, req_data['state'])
        elif request_type == 'mean_by_category_request':
            return self.mean_by_category_request(question)
        elif request_type == 'state_mean_by_category_request':
            return self.state_mean_by_category_request(question)

    def states_mean_request(self, question: str):
        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get all states
        states = list(set([d['LocationDesc'] for d in filtered_data]))

        # Get the mean for each state
        mean_by_state = {}

        # Go through all the data
        for d in filtered_data:
            state = d['LocationDesc']
            mean = float(d['Data_Value'])

            # Add state to the dictionary if it doesn't exist
            if state not in mean_by_state:
                mean_by_state[state] = { 'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            mean_by_state[state]['mean'] += mean
            mean_by_state[state]['divisor'] += 1

        # Calculate the mean for each state
        for state in states:
            mean_by_state[state] = mean_by_state[state]['mean'] / mean_by_state[state]['divisor']

        return mean_by_state
    
    def state_mean_request(self, question: str, state: str):
        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Calculate the mean for the state
        values_sum = sum(float(d['Data_Value']) for d in filtered_data)
        count = len(filtered_data)

        mean = values_sum / count if count else 0
            
        return {state: mean}
    
    def best5_request(self, question: str):
        # Filter data based on the question and year range
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Combine aggregation and mean calculation for each state
        state_aggregates = {}
        for entry in filtered_data:
            state = entry['LocationDesc']
            value = float(entry['Data_Value'])

            # Add state to the dictionary if it doesn't exist
            if state not in state_aggregates:
                state_aggregates[state] = {'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            state_aggregates[state]['mean'] += value
            state_aggregates[state]['divisor'] += 1

        # Calculate mean for each state in a more compact form
        state_means = {state: aggregates['mean'] / aggregates['divisor'] for state, aggregates in state_aggregates.items()}

        # Determine if the best values are the highest or lowest
        min_max = 'min' if question in self.questions_best_is_min else 'max'

        # Sort states by their mean values
        if min_max == 'max':
            sorted_means = dict(sorted(state_means.items(), key=lambda item: item[1], reverse=True))
        else:
            sorted_means = dict(sorted(state_means.items(), key=lambda item: item[1], reverse=False))

        # Select the top 5 states based on the sorted order
        best5_states = dict(list(sorted_means.items())[:5])

        return best5_states

    
    def worst5_request(self, question: str):
        # Filter data based on the question and year range
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Combine aggregation and mean calculation for each state
        state_aggregates = {}
        for entry in filtered_data:
            state = entry['LocationDesc']
            value = float(entry['Data_Value'])
            if state not in state_aggregates:
                state_aggregates[state] = {'mean': 0, 'divisor': 0}
            state_aggregates[state]['mean'] += value
            state_aggregates[state]['divisor'] += 1

        # Calculate mean for each state in a more compact form
        state_means = {state: aggregates['mean'] / aggregates['divisor'] for state, aggregates in state_aggregates.items()}

        # Determine if the best values are the highest or lowest
        min_max = 'min' if question in self.questions_best_is_min else 'max'

        # Sort states by their mean values
        if min_max == 'max':
            sorted_means = dict(sorted(state_means.items(), key=lambda item: item[1], reverse=True))
        else:
            sorted_means = dict(sorted(state_means.items(), key=lambda item: item[1], reverse=False))

        # Select the top 5 states based on the sorted order
        worst5_states = dict(list(sorted_means.items())[-5:])

        return worst5_states
    
    def global_mean_request(self, question: str):
        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the global mean
        values_sum = sum([float(d['Data_Value']) for d in filtered_data])
        count = len(filtered_data)

        mean = values_sum / count if count else 0
        
        return {'global_mean': mean}
    
    def diff_from_mean_request(self, question: str):
        # Get the global mean
        global_mean = self.global_mean_request(question)

        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Compute the difference from the global mean for each state
        diff_by_state = {}

        for d in filtered_data:
            state = d['LocationDesc']
            mean = float(d['Data_Value'])
            diff_by_state[state] = mean - global_mean
        
        return diff_by_state
    
    def state_diff_from_mean_request(self, question: str, state: str):
        # Get the global mean
        global_mean = self.global_mean_request(question)
        
        # Get all data corresponding to the question and state
        filtered_data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get the state mean
        values_sum = sum([float(d['Data_Value']) for d in filtered_data])
        count = len(filtered_data)
        state_mean = values_sum / count if count else 0

        # Calculate the difference from the global mean
        diff = state_mean - global_mean

        return {state: diff}
    
    def mean_by_category_request(self, question: str, category: str):   
        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get all categories
        categories = list(set([d['StratificationCategory1'] for d in filtered_data]))

        # Get the mean for each category
        mean_by_category = {}
        mean_by_category_divisor = {}

        for category in categories:
            # Get the data for the category
            category_data = [d for d in filtered_data if d['StratificationCategory1'] == category]

            # Compute the mean
            values_sum = sum([float(d['Data_Value']) for d in category_data])
            count = len(category_data)
            mean = values_sum / count if count else 0

            # Compute the mean for the category
            mean_by_category[category] += mean
            mean_by_category_divisor[category] += 1

        # Calculate the mean for each category
        for category in categories:
            mean_by_category[category] /= mean_by_category_divisor[category]

        return mean_by_category
    
    def state_mean_by_category_request(self, question: str, state: str, category: str):
        # Get all data corresponding to the question and state
        filtered_data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Get all categories
        categories = list(set([d['StratificationCategory1'] for d in filtered_data]))

        # Get the mean for each category
        mean_by_category = {}
        mean_by_category_divisor = {}

        for category in categories:
            # Get the data for the category
            category_data = [d for d in filtered_data if d['StratificationCategory1'] == category]

            # Compute the mean
            values_sum = sum([float(d['Data_Value']) for d in category_data])
            count = len(category_data)
            mean = values_sum / count if count else 0

            # Compute the mean for the category
            mean_by_category[category] += mean
            mean_by_category_divisor[category] += 1

        # Calculate the mean for each category
        for category in categories:
            mean_by_category[category] /= mean_by_category_divisor[category]

        return mean_by_category