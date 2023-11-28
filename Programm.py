# Программа на Python для создания блокчейна - База клиентов парфюмерного магазина
# Для временной метки
import datetime
# Вычисление хэша для добавления цифровой подписи к блокам
import hashlib
# Для хранения данных в блокчейне
import json
# Flask предназначен для создания веб-приложения, а jsonify - для
# отображения блокчейнаn
from flask import Flask, jsonify
# Для подключения к базе postgres
import psycopg2
# Для работы с датафреймом
import pandas as pd

class Blockchain:
# Эта функция ниже создана для создания самого первого блока и установки его хэша равным "0"

    # функция для работы с базой данных postgres
    def postgre ():
        conn = psycopg2.connect(dbname = 'test', user='postgres', password='12345', host='localhost')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM "clients"')
        df = cursor.fetchall()
        df = pd.DataFrame(df)
        #df = df.drop(0, axis=1)
        print (df)
        return df
    
    def __init__(self):
        self.chain = []
        self.create_block(proof=1, previous_hash='0')
        
# Эта функция ниже создана для добавления дополнительных блоков в цепочку
    def create_block(self, proof, previous_hash):
        df = Blockchain.postgre()
        block = {'index': len(self.chain) + 1,
                 'first_name': str(df[1].iloc[len(self.chain)]),
                 'age': str(df[2].iloc[len(self.chain)]),
                 'email': str(df[3].iloc[len(self.chain)]),
                 'sex': str(df[4].iloc[len(self.chain)]),
                 'date_birth': str(df[5].iloc[len(self.chain)]),
                 'favourite_category': str(df[6].iloc[len(self.chain)]), 
                 'bonuses': str(df[7].iloc[len(self.chain)]),
                 'timestamp': str(df[8].iloc[len(self.chain)]),
                 'refund':  str(df[9].iloc[len(self.chain)]),
                 'proof':proof,
                 'previous_hash': previous_hash}
        self.chain.append(block)
        return block
    
# Эта функция ниже создана для отображения предыдущего блока
    def print_previous_block(self):
        return self.chain[-1]
    
# Эти функции для проверки работы и используются для успешного майнинга блока с хешем
    def proof_of_work(self, previous_proof):
        new_proof = 1
        check_proof = False
        while check_proof is False:
            hash_operation = hashlib.sha256(
                str(new_proof**2 - previous_proof**2).encode()).hexdigest()
            if hash_operation[:5] == '00000':
                check_proof = True
            else:
                new_proof += 1
        return new_proof
    
    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()
    
    def chain_valid(self, chain):
        previous_block = chain[0]
        block_index = 1
        while block_index < len(chain):
            block = chain[block_index]
            if block['previous_hash'] != self.hash(previous_block):
                return False
            previous_proof = previous_block['proof']
            proof = block['proof']
            hash_operation = hashlib.sha256(
                str(proof**2 - previous_proof**2).encode()).hexdigest()
            if hash_operation[:5] != '00000':
                return False
            previous_block = block
            block_index += 1
        return True
  
# Создание веб-приложения с использованием flask
app = Flask(__name__)

# Создаем объект класса blockchain
blockchain = Blockchain()

# Майнинг нового блока
@app.route('/mine_block', methods=['GET'])

def mine_block():
    previous_block = blockchain.print_previous_block()
    previous_proof = previous_block['proof']
    proof = blockchain.proof_of_work(previous_proof)
    previous_hash = blockchain.hash(previous_block)
    block = blockchain.create_block(proof, previous_hash)
    response = {'message': 'A block is MINED',
                'index': block['index'],
                'timestamp': block['timestamp'],
                'proof': block['proof'],
                'previous_hash': block['previous_hash']}
    print (response)
    return jsonify(response), 200

# Отобразить блокчейн в формате json
@app.route('/display_chain', methods=['GET'])

def display_chain():
    response = {'chain': blockchain.chain,
                'length': len(blockchain.chain)}
    return jsonify(response), 200
# Проверка валидности блокчейна
@app.route('/valid', methods=['GET'])


def valid():
    valid = blockchain.chain_valid(blockchain.chain)
    if valid:
        response = {'message': 'The Blockchain is valid.'}
    else:
        response = {'message': 'The Blockchain is not valid.'}
    return jsonify(response), 200

# Запустите сервер flask локально
if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000)
