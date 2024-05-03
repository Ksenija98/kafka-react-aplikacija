from kafka import KafkaConsumer
import requests, json

# Inicijalizacija Kafka consumer-a

consumer = KafkaConsumer('topic2', bootstrap_servers='localhost:9092', group_id='consumer-group')

# Inicijalizacija Node.js server endpoint-a

nodejs_server_url = 'http://localhost:8000/api/receive-data'

# Citanje i obrada poruka iz Kafka topic-a

for message in consumer:
    # Dekodiranje i ucitavanje JSON podataka iz Kafka poruke
    srednja_temperatura = message.value.decode('utf-8')
    srednja_temperatura_json = json.loads(srednja_temperatura)
    
    # Slanje podataka na Node.js server
    response = requests.post(nodejs_server_url, json=srednja_temperatura_json)
    
    # Ispisivanje odgovora od Node.js servera
    print(f"Odgovor od Node.js servera: {response.text}")

# Zatvaranje consumer-a

consumer.close()






