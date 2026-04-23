# Databricks notebook source
# === AI-Powered Natural Language to SQL Assistant ===

import openai, json, numpy as np
from datetime import datetime
from databricks import sql as dbc
from numpy.linalg import norm

# --- Configuration & Environment Setup ---
# All database and connection specifics are consolidated here
openai.api_key = "YOUR_OPENAI_KEY"
LLM_MODEL = "gpt-4o"
EMBEDDING_MODEL = "text-embedding-3-small"

# Databricks Connection Config
DB_CATALOG_SCHEMA = "your_catalog.your_schema" # Replace with your target schema
host = "://databricks.com"
http_path = "/sql/1.0/warehouses/your-warehouse-id"
token = "your-access-token"

# Establish the Databricks SQL connection
conn = dbc.connect(server_hostname=host, http_path=http_path, access_token=token)
cursor = conn.cursor()

def run_query(sql_query: str, retry_count=0) -> dict:
    """
    Executes SQL queries against the specified Databricks schema.
    Includes a security filter to block write operations and a 
    recursive self-correction loop to fix syntax errors in real-time.
    """
    # Security: Ensure only read operations are processed
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER"]
    if any(k in sql_query.upper() for k in forbidden):
        return {"columns": [], "rows": [], "error": "Security: Write operations blocked."}

    try:
        # Direct session to the configured schema
        cursor.execute(f"USE {DB_CATALOG_SCHEMA}")
        cursor.execute(sql_query)
        
        cols = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return {"columns": cols, "rows": rows}
    
    except Exception as e:
        # Self-Correction Loop: If the SQL fails, pass the error back to the LLM to debug
        if retry_count < 1:
            print(f"🔄 SQL Error encountered. Prompting {LLM_MODEL} to self-correct...")
            fix_resp = openai.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": f"You are a SQL expert for Databricks. The query failed. Fix the syntax for the schema {DB_CATALOG_SCHEMA} and return ONLY the corrected SQL string."},
                    {"role": "user", "content": f"Failed SQL: {sql_query}\nError: {e}"}
                ]
            )
            return run_query(fix_resp.choices[0].message.content, retry_count + 1)
        
        return {"columns": [], "rows": [], "error": str(e)}

# --- Semantic Indexing (RAG Setup) ---
# This block runs once to build a searchable knowledge base of your database structure
if "embeddings" not in globals():
    tables = [row[1] for row in run_query(f"SHOW TABLES IN {DB_CATALOG_SCHEMA}")["rows"]]
    metadata_docs = []
    
    print(f"🧠 Building semantic index for {DB_CATALOG_SCHEMA}...")
    for tbl in tables:
        # 1. Capture Database DDL (Table structure and relationships)
        ddl_res = run_query(f"SHOW CREATE TABLE {DB_CATALOG_SCHEMA}.{tbl}")
        ddl = ddl_res["rows"][0][0] if ddl_res["rows"] else "DDL not available"
        
        # 2. Capture column descriptions for context
        desc = run_query(f"DESCRIBE TABLE {DB_CATALOG_SCHEMA}.{tbl}")
        col_list = ", ".join([row[0] for row in desc["rows"]])
        
        # 3. AI-Generated Synonyms
        # Bridges the gap between business terminology and technical column names
        syn_resp = openai.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role": "user", "content": f"Table: {tbl}. Columns: {col_list}. List 10 business synonyms or natural language terms users might use to ask about this data."}]
        )
        synonyms = syn_resp.choices[0].message.content
        
        # 4. Data Sampling
        # Gives the LLM a 'preview' of the data values to ensure logical accuracy
        sample_rows = run_query(f"SELECT * FROM {DB_CATALOG_SCHEMA}.{tbl} LIMIT 3")["rows"]
        sample_text = "\n".join([str(r) for r in sample_rows])
        
        # Consolidate metadata into a single searchable document
        doc = f"Table: `{tbl}`\nSynonyms: {synonyms}\nDDL:\n{ddl}\nSample Data:\n{sample_text}"
        metadata_docs.append(doc[:20000]) # Protect context window limits

    # Create vector embeddings for the metadata
    embeddings = [
        (doc, np.array(openai.embeddings.create(model=EMBEDDING_MODEL, input=doc).data[0].embedding))
        for doc in metadata_docs
    ]

def get_relevant_schema(query, k=5):
    """Retrieves the most relevant table metadata for a given natural language query."""
    qvec = np.array(openai.embeddings.create(model=EMBEDDING_MODEL, input=query).data[0].embedding)
    sims = [(doc, float(np.dot(qvec, ev)/(norm(qvec)*norm(ev)))) for doc, ev in embeddings]
    sims.sort(key=lambda x: x[1], reverse=True)
    return [doc for doc, _ in sims[:k]]

# --- Assistant Logic ---

function_spec = [{
    "name": "run_query",
    "description": f"Execute a SQL query on {DB_CATALOG_SCHEMA} to retrieve data.",
    "parameters": {
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "The Databricks SQL statement to run."}
        },
        "required": ["sql"]
    }
}]

# Persistent system instructions for dialect and formatting
messages = [{"role": "system", "content": (
    f"You are a business data assistant for the {DB_CATALOG_SCHEMA} schema.\n"
    "When generating SQL for Databricks, do not use INTERVAL syntax. Use date_sub(date, int_days).\n"
    "Format all final responses in valid HTML using <h3>, <p>, and <ul> tags. Suggest a follow-up question."
)}]

print("🚀 Assistant is online. Ask a question about your data...")

while True:
    user_input = input("\nYou: ")
    if user_input.lower() in ("exit", "quit"): break

    # Retrieve specific schema context for the question
    schema_context = "\n\n".join(get_relevant_schema(user_input))
    
    chat_payload = messages + [
        {"role": "system", "content": f"Database Context:\n{schema_context}"},
        {"role": "user", "content": user_input}
    ]

    # Initial LLM call to decide if a query is needed
    response = openai.chat.completions.create(
        model=LLM_MODEL,
        messages=chat_payload,
        functions=function_spec,
        function_call="auto"
    )
    
    msg = response.choices[0].message
    
    if msg.function_call:
        sql = json.loads(msg.function_call.arguments).get("sql")
        query_result = run_query(sql)
        
        # Generate the final narrative answer based on the data returned
        final_summary = openai.chat.completions.create(
            model=LLM_MODEL,
            messages=chat_payload + [
                msg,
                {"role": "function", "name": "run_query", "content": json.dumps(query_result, default=str)}
            ]
        )
        print(f"\nAssistant: {final_summary.choices[0].message.content}")
    else:
        print(f"\nAssistant: {msg.content}")