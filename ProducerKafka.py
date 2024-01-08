from confluent_kafka import Producer
import faker
import time

bootstrap_servers = 'localhost:9092'
topic = 'meu-topico2'

def gerar_dados():
    fake = faker.Faker('pt_BR')
    nome = fake.name()
    email = fake.email()
    endereco = fake.address()
    telefone = fake.phone_number()
    data_aniversario = fake.date_of_birth()

    return f"Nome: {nome}\nEmail: {email}\nEndereço: {endereco}\nTelefone: {telefone}\nData de Aniversário: {data_aniversario}\n\n"

def producer_example():
    p = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        # Enviar mensagens para o tópico
        for i in range(50):
            p.produce(topic, gerar_dados())
            p.flush()  # Garante que as mensagens são entregues
            time.sleep(15)

        print("Mensagens enviadas com sucesso!")

    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")



producer_example()
