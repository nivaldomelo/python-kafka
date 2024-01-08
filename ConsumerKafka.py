from confluent_kafka import Consumer


bootstrap_servers = 'localhost:9092'
topic = 'meu-topico2'


def consumer_example():
    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'  # Pode ser 'latest' ou 'earliest'
    })
    c.subscribe([topic])

    try:
        with open('dados.txt', 'wb') as file:
            while True:
                msg = c.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print(f"Erro ao receber mensagem: {msg.error()}")
                    continue
                else:
                    data = msg.value()
                    file.write(data)
                    print(f"{msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass

    finally:
        c.close()


consumer_example()
