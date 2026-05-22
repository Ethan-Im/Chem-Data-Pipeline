import pandas as pd
import json
import time
from kafka import KafkaProducer

# ESOL 데이터셋 다운로드 (DeepChem 공개 데이터)
url = "https://raw.githubusercontent.com/deepchem/deepchem/master/datasets/delaney-processed.csv"
df = pd.read_csv(url)

print(f"데이터 로드 완료: {len(df)}개 분자")
print(df.head(3))

# Kafka Producer 연결
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 한 행씩 Kafka 토픽으로 전송
for idx, row in df.iterrows():
    message = {
        "compound": row["Compound ID"],
        "smiles": row["smiles"],
        "solubility": row["measured log solubility in mols per litre"],
        "mol_weight": row["Molecular Weight"],
        "timestamp": time.time()
    }
    producer.send("chem-raw", value=message)
    print(f"전송: {message['compound']}")
    time.sleep(0.5)  # 0.5초 간격으로 스트리밍

producer.flush()
print("전송 완료!")
