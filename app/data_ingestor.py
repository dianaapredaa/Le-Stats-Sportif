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
                    "LocationDesc": values[4],
                    "Question": values[8],
                    "Data_Value": values[11],
                    "StratificationCategory1": values[30],
                    "Stratification1": values[31],
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
            return self.state_mean_by_category_request(question, req_data['state'])

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

        # Calculate the mean
        mean = values_sum / count if count else 0
            
        return {state: mean}
    
    def best5_request(self, question: str):
        # Filter data based on the question and year range
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Combine aggregation and mean calculation for each state
        state_means = {}
        for entry in filtered_data:
            state = entry['LocationDesc']
            value = float(entry['Data_Value'])

            # Add state to the dictionary if it doesn't exist
            if state not in state_means:
                state_means[state] = {'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            state_means[state]['mean'] += value
            state_means[state]['divisor'] += 1

        # Calculate mean for each state in a more compact form
        best = {state: aggregates['mean'] / aggregates['divisor'] for state, aggregates in state_means.items()}

        # Determine if the best values are the highest or lowest
        min_max = 'min' if question in self.questions_best_is_min else 'max'

        # Sort states by their mean values
        if min_max == 'max':
            best_sorted = dict(sorted(best.items(), key=lambda item: item[1], reverse=True))
        else:
            best_sorted = dict(sorted(best.items(), key=lambda item: item[1], reverse=False))

        # Select the top 5 states based on the sorted order
        best5_states = dict(list(best_sorted.items())[:5])

        return best5_states

    
    def worst5_request(self, question: str):
        # Filter data based on the question and year range
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Combine aggregation and mean calculation for each state
        state_means = {}
        for d in filtered_data:
            state = d['LocationDesc']
            value = float(d['Data_Value'])

            # Add state to the dictionary if it doesn't exist
            if state not in state_means:
                state_means[state] = {'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            state_means[state]['mean'] += value
            state_means[state]['divisor'] += 1

        # Calculate mean for each state in a more compact form
        worst = {state: aggregates['mean'] / aggregates['divisor'] for state, aggregates in state_means.items()}

        # Determine if the best values are the highest or lowest
        min_max = 'min' if question in self.questions_best_is_min else 'max'

        # Sort states by their mean values
        if min_max == 'max':
            worst_sorted = dict(sorted(worst.items(), key=lambda item: item[1], reverse=True))
        else:
            worst_sorted = dict(sorted(worst.items(), key=lambda item: item[1], reverse=False))

        # Select the top 5 states based on the sorted order
        worst5_states = dict(list(worst_sorted.items())[-5:])

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
        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Calculate the global mean
        global_mean = sum([float(d['Data_Value']) for d in filtered_data]) / len(filtered_data) if filtered_data else 0

        # Calculate mean for each state
        state_means = {}
        for d in filtered_data:
            state = d['LocationDesc']
            value = float(d['Data_Value'])

            # Add state to the dictionary if it doesn't exist
            if state not in state_means:
                state_means[state] = {'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            state_means[state]['mean'] += value
            state_means[state]['divisor'] += 1
        
        # Calculate mean for each state and compute the difference from the global mean
        diff_from_global_mean = {}
        for state, data in state_means.items():
            state_mean = data['mean'] / data['divisor']
            diff_from_global_mean[state] = global_mean - state_mean

        return diff_from_global_mean
        
    def state_diff_from_mean_request(self, question: str, state: str):
        # Filter data for the specified question and year range
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        # Calculate the global mean
        value_sum = sum(float(d['Data_Value']) for d in filtered_data)
        count = len(filtered_data)

        global_mean = value_sum / count if count else 0
        
        # Filter data further for the specified state
        state_filtered_data = [d for d in filtered_data if d['LocationDesc'] == state]
        
        # Calculate the mean for the specified state
        value_sum = sum(float(d['Data_Value']) for d in state_filtered_data)
        count = len(state_filtered_data)

        state_mean = value_sum / count if count else 0
        
        # Calculate the difference from the global mean
        diff = global_mean - state_mean

        # Return the difference in the expected format
        return {state: diff}

    def mean_by_category_request(self, question: str):
        # Get all data corresponding to the question
        filtered_data = [d for d in self.data if d['Question'] == question and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']

        mean_by_category = {}

        for d in filtered_data:
            state = d['LocationDesc']
            value = float(d['Data_Value'])
            category = d['StratificationCategory1']
            category_segment = d['Stratification1']

            # Skip if category or category_segment is empty
            if category == '' or category_segment == '':
                continue

            # Add category to the dictionary if it doesn't exist
            if category not in mean_by_category:
                mean_by_category[category] = {}

            # Add category segment to the dictionary if it doesn't exist
            if category_segment not in mean_by_category[category]:
                mean_by_category[category][category_segment] = {}

            # Initialize the state dictionary if it doesn't exist
            if state not in mean_by_category[category][category_segment]:
                mean_by_category[category][category_segment][state] = {'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            mean_by_category[category][category_segment][state]['mean'] += value
            mean_by_category[category][category_segment][state]['divisor'] += 1

        # Prepare a new dictionary to hold the mean values
        mean_values = {}

        # Calculate the mean for each category, segment, and state, and store in the new dictionary
        for category, category_segments in mean_by_category.items():
            for category_segment, states in category_segments.items():
                for state, data in states.items():
                    # Initialize the category dictionary if it doesn't exist
                    if category not in mean_values:
                        mean_values[category] = {}

                    # Initialize the category_segment dictionary if it doesn't exist
                    if category_segment not in mean_values[category]:
                        mean_values[category][category_segment] = {}
                    
                    # Calculate the mean and store it
                    calculated_mean = data['mean'] / data['divisor'] if data['divisor'] else 0
                    mean_values[category][category_segment][state] = calculated_mean

        # Normalize result to: "('state', 'category', 'category_segment')": mean
        mean_by_category_normalized = {}

        for category in mean_values:
            for category_segment in mean_values[category]:
                for state in mean_values[category][category_segment]:
                    # Format the key as a string that looks like a tuple
                    key_formatted = f"('{state}', '{category}', '{category_segment}')"
                    
                    # Assign the mean value to the formatted string key
                    mean_value = mean_values[category][category_segment][state]
                    mean_by_category_normalized[key_formatted] = mean_value

        return mean_by_category_normalized
        
    def state_mean_by_category_request(self, question: str, state: str):
        # Get all data corresponding to the question and state
        filtered_data = [d for d in self.data if d['Question'] == question and d['LocationDesc'] == state and d['YearStart'] >= '2011' and d['YearEnd'] <= '2022']
        
        mean_by_category = {}

        for d in filtered_data:
            value = float(d['Data_Value'])
            category = d['StratificationCategory1']
            category_segment = d['Stratification1']

            # Skip if category or category_segment is empty
            if category == '' or category_segment == '':
                continue

            # Add category to the dictionary if it doesn't exist
            if category not in mean_by_category:
                mean_by_category[category] = {}

            # Add category segment to the dictionary if it doesn't exist
            if category_segment not in mean_by_category[category]:
                mean_by_category[category][category_segment] = {'mean': 0, 'divisor': 0}

            # Add the value to the total and increment the count
            mean_by_category[category][category_segment]['mean'] += value
            mean_by_category[category][category_segment]['divisor'] += 1

        # Prepare a new dictionary to hold the mean values
        mean_values = {}

        # Calculate the mean for each category, segment, and state, and store in the new dictionary
        for category, category_segments in mean_by_category.items():
            for category_segment, data in category_segments.items():
                # Calculate the mean and store it
                calculated_mean = data['mean'] / data['divisor'] if data['divisor'] else 0

                # Format the result
                key_formatted = f"('{category}', '{category_segment}')"
                mean_values[key_formatted] = calculated_mean
                
        return {state : mean_values}