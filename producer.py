from kafka import KafkaProducer  
import requests, json

# Inicijalizacija Kafka producer-a 

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')  

# Konfiguracija API-ja za vreme u Beogradu

api_key = 'ea3051e935c22f344513efd8f59f10f3'  

city = 'Belgrade'  

lat=44.787197 

lon=20.457273 

weather_api_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'  

# Hvatanje podataka sa API-ja za vreme

response = requests.get(weather_api_url)  

weather_data = json.dumps(response.json())  

# Slanje podataka u topic1  

producer.send('topic1', value=weather_data.encode('utf-8'))  

# Zatvaranje producer-a  

producer.close() 