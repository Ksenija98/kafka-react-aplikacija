from kafka import KafkaConsumer, KafkaProducer
import json, time

# Inicijalizacija Kafka consumer-a

consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='consumer-producer-group')

# Inicijalizacija Kafka producer-a  

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

# Inicijalizacija varijabli

broj_primljenih_temperatura = 0
ukupna_temperatura = 0

# Citanje, obrada i slanje podataka

for message in consumer:
    # Dekodiranje i ucitavanje JSON podataka iz Kafka poruke
    podaci_o_vremenu = message.value.decode('utf-8')
    podaci_o_vremenu_json = json.loads(podaci_o_vremenu)

    # Izmena vrednosti varijabli
    broj_primljenih_temperatura += 1
    ukupna_temperatura += podaci_o_vremenu_json.get('main', {}).get('temp', 0)
    
    temperatura = podaci_o_vremenu_json.get('main', {}).get('temp', 0)
    print(f"Poruka broj: {broj_primljenih_temperatura} -> Temperatura: {temperatura}")

    # Ako je primljeno 5 poruka, racuna se njihova prosecna temperatura i upisuje u topic2
    if broj_primljenih_temperatura == 5:
        srednja_temperatura = ukupna_temperatura / 5 if broj_primljenih_temperatura > 0 else 0

        # Slanje podataka u topic2
        sredjeni_podaci = {'srednja_temperatura': srednja_temperatura}
        producer.send('topic2', value=json.dumps(sredjeni_podaci).encode('utf-8'))

        print(f"Srednja temperatura: {srednja_temperatura}")

        # Postavljanje pocetnih vrednosti varijabli
        broj_primljenih_temperatura = 0
        ukupna_temperatura = 0

# Zatvaranje consumer-a i producer-a

consumer.close()
producer.close()