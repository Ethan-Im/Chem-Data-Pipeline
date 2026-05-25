from kafka import KafkaConsumer
import json

def check_quality(data):
    """
    Real-time data quality check for incoming molecular data.
    """
    issues = []

    # Check 1: Missing required fields
    required_fields = ["compound", "smiles", "solubility", "mol_weight"]
    for field in required_fields:
        if field not in data or data[field] is None:
            issues.append(f"MISSING_FIELD: {field}")

    # Check 2: Molecular weight out of valid range
    mol_weight = data.get("mol_weight", 0)
    if mol_weight <= 0 or mol_weight > 2000:
        issues.append(f"INVALID_MOL_WEIGHT: {mol_weight}")

    # Check 3: Solubility out of valid range
    solubility = data.get("solubility", 0)
    if solubility < -20 or solubility > 5:
        issues.append(f"INVALID_SOLUBILITY: {solubility}")

    # Check 4: SMILES string too short
    smiles = data.get("smiles", "")
    if len(smiles) < 2:
        issues.append(f"INVALID_SMILES: too short")

    return issues

def main():
    print("=== Flink-style Quality Check Pipeline Started ===")
    print("Connecting to Kafka topic: chem-raw")

    consumer = KafkaConsumer(
        'chem-raw',
        bootstrap_servers='kafka:29092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    pass_count = 0
    fail_count = 0

    for message in consumer:
        data = message.value
        issues = check_quality(data)

        if issues:
            fail_count += 1
            print(f"[ALERT] {data.get('compound')} | Issues: {issues}")
        else:
            pass_count += 1
            print(f"[OK]    {data.get('compound')} passed all checks")

        total = pass_count + fail_count
        if total % 100 == 0:
            print(f"\n--- Stats: {pass_count} passed / {fail_count} failed / {total} total ---\n")

if __name__ == "__main__":
    main()
