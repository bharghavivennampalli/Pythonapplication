import requests
import redis
import json
import matplotlib.pyplot as plt

class DataProcessor:
   
    def __init__(self):
        self.redis_client = redis.Redis(host='redis-17790.c323.us-east-1-2.ec2.cloud.redislabs.com',
                                        port=17790,
                                        password='4fBi1e9lmkrJ1IFxZUwE2nOyjdo0RlZb')

    def fetch_apidata(self, api_url):
        
        response = requests.get(api_url)
        return response.json()

    def store_inredis(self, key, data):
        
        self.redis_client.set(key, json.dumps(data))

    def read_fromredis(self, key):
       
        data = self.redis_client.get(key)
        return json.loads(data.decode('utf-8')) if data else None

    def plot_topdrivers(self, data, num_drivers=10):
       
        drivers = data['MRData']['DriverTable']['Drivers']
        drivers_sorted = sorted(drivers, key=lambda x: x['familyName'])
        top_drivers = drivers_sorted[:num_drivers]

        driver_names = [f"{driver['givenName']} {driver['familyName']}" for driver in top_drivers]
        driver_dobs = [driver['dateOfBirth'] for driver in top_drivers]

        plt.bar(driver_names, driver_dobs)
        plt.xlabel('Driver')
        plt.ylabel('Date of Birth')
        plt.title('Top 10 Drivers by Date of Birth')
        plt.xticks(rotation=45)
        plt.show()

    def aggregate_data(self, data, data_key):
        """
        Aggregates data based on the specified key.

        Parameters:
            data (dict): The data to aggregate.
            data_key (str): The key to aggregate the data on.

        Returns:
            dict: The aggregated data.
        """
        drivers = data['MRData']['DriverTable']['Drivers']
        aggregated_data = {}

        for driver in drivers:
            key = driver[data_key]
            if key in aggregated_data:
                aggregated_data[key] += 1
            else:
                aggregated_data[key] = 1
        
        return aggregated_data
    
    def search_data(self, data, keyword):
        
        drivers = data['MRData']['DriverTable']['Drivers']
        matched_drivers = []

        for driver in drivers:
            if keyword.lower() in driver['driverId'].lower() or \
               keyword.lower() in driver['givenName'].lower() or \
               keyword.lower() in driver['familyName'].lower():
                matched_drivers.append(driver)
        
        print(f" records are found {matched_drivers}")

# Example usage:
if __name__ == "__main__":
    api_url = "http://ergast.com/api/f1/drivers.json"
    processor = DataProcessor()

    # Fetch data from API
    data = processor.fetch_apidata(api_url)

    # Store data in Redis
    processor.store_inredis('f1_drivers', data)

    # Read data from Redis
    data_from_redis = processor.read_fromredis('f1_drivers')

    # Perform processing
    if data_from_redis:
        processor.plot_topdrivers(data_from_redis) 
        processor.search_data(data_from_redis, 'alonso')
        aggregated_data = processor.aggregate_data(data_from_redis, 'nationality')
        print("Aggregated data:", aggregated_data)
    else:
        print("No data found in Redis.")
