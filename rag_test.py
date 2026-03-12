import yaml
import os
import chromadb
import anthropic

documents = []

models_path = "/Users/rashedulkabir/Documents/Code/EMS Incidents/dbt/models/marts"

for root, dirs, files in os.walk(models_path):
    for file in files:
        if file.endswith(".yml") or file.endswith(".yaml"):
            with open(os.path.join(root, file)) as f:
                data = yaml.safe_load(f)

                for model in data.get("models", []):
                    model_text = f"""
                    Model: {model['name']}
                    Description: {model.get('description','')}
                    """

                    for col in model.get("columns", []):
                        model_text += f"""
                        Column: {col['name']}
                        Description: {col.get('description','')}
                        """

                    documents.append(model_text)


client = chromadb.Client()

collection = client.create_collection("ems_metadata")

for i, doc in enumerate(documents):
    collection.add(
        documents=[doc],
        ids=[str(i)]
    )

def retrieve_context(question):

    results = collection.query(
        query_texts=[question],
        n_results=3
    )

    return results["documents"]


if __name__ == "__main__":
    print(f"Loaded {len(documents)} documents into ChromaDB\n")

    test_questions = [
        "What is the average response time by borough?",
        "How many incidents happened during special events?",
        "What are the most common call types?",
    ]

    for q in test_questions:
        print(f"Q: {q}")
        context = retrieve_context(q)
        for doc in context[0]:
            first_line = [line.strip() for line in doc.strip().splitlines() if line.strip()][0]
            print(f"  → {first_line}")
        print()