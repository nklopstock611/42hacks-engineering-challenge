import pandas as pd
import requests

# Load the ground truth data
def load_ground_truth(filename):
    try:
        return pd.read_csv(filename)
    except Exception as e:
        print(f"Error loading the ground truth CSV: {e}")
        return None

# Make a GET request to the Flask app
def get_data_from_api(user_id, endpoint):
    '''
    Change URL To test API against ground truth 
    '''
    url = f"http://127.0.0.1:5000{endpoint}/{user_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve data for user_id {user_id}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# Compare API data with ground truth and compute accuracy
def compare_data(ground_truth, user_id, endpoint):
    api_data = get_data_from_api(user_id, endpoint)
    matches = 0
    total = 0
    if api_data is not None:
        # Assuming the ground truth data and API response are dictionaries
        gt_row = ground_truth.loc[ground_truth['user_id'] == user_id].to_dict('records')[0]
        keys = ['airport_id'] if endpoint=='/nearest_airports' else ['wikipedia_link']
        for key in keys:
            total += 1
            if key in api_data and gt_row[key] == api_data[key]:
                matches += 1
            else:
                print(f"Mismatch for {key}: Ground Truth - {gt_row[key]}, API - {api_data.get(key, 'Data not found')}")
    return matches, total

if __name__=="__main__":
    #first challenge
    ground_truth_wiki=pd.read_csv('./airports_wiki_sample.csv')
    '''
    Change File To test airport data cleaning 
    '''
    solution=pd.read_csv('./airports_wiki.csv')

    print(f" First Stage Accuracy: {(len(set(ground_truth_wiki.id).intersection(set(solution.id)))/len(set(ground_truth_wiki.id)))*100:.2f}%")

    #second challenge
    ground_truth_file = 'ground_truth_sample.csv'  # specify your CSV file name
    ground_truth = load_ground_truth(ground_truth_file)
    if ground_truth is not None:
        user_ids = ground_truth['user_id'].unique()  # Assuming 'user_id' column exists
        total_matches = 0
        total_comparisons = 0
        for user_id in user_ids:
            #print(f"Comparing data for user ID {user_id}")
            matches, total = compare_data(ground_truth, user_id, '/nearest_airports')
            total_matches += matches
            total_comparisons += total
        # Calculate accuracy
        if total_comparisons > 0:
            accuracy = (total_matches / total_comparisons) * 100
            print(f" Second Stage Accuracy: {accuracy:.2f}%")
        else:
            print("No comparisons were made.")
    

    
    
    