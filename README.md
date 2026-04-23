# semantic-bridge-sql

### The Problem
Most Text-to-SQL implementations fail because they treat a database like a dictionary of column names. Without understanding the **intent** behind the data, the **relationships** between tables, or the **dialect-specific** syntax (like Databricks SQL), models create queries that are syntactically correct but logically "hallucinated."

### The Solution: The "Semantic Bridge"
This project implements a **Semantic Bridge** approach. Instead of a "black box" query generator, this engine builds a rich, searchable index of your database’s semantics. It mirrors the workflow of a human data analyst by observing data patterns before writing code.

#### How It Works:
1.  **Semantic RAG (Retrieval-Augmented Generation):** Instead of cramming the entire database schema into every prompt (which creates "noise"), the script uses vector embeddings to find the specific table metadata most relevant to the user's question.
2.  **Automated AI Synonyms:** During initialization, the system uses AI to analyze your tables and generate business-friendly keywords. This allows the system to find the `fact_revenue` table even if the user asks about "Top Line" or "Earnings."
3.  **Real-Data Sampling:** The LLM is provided with a "preview" of actual data values. This allows it to understand data formats (e.g., date strings vs. timestamps) and category values before the query is ever written.
4.  **Dialect Guardrails:** Explicit system instructions force the LLM to adhere to specific warehouse syntax (e.g., Databricks-specific `date_sub` functions), bypassing the most common cause of SQL syntax errors.
5.  **Autonomous Self-Correction:** If a query fails, the script captures the traceback error and feeds it back to the LLM. The model then diagnoses its own mistake and fixes the query in real-time.

---

### Security & Governance
When deploying this in a production environment, keep the following "Safe-by-Design" principles in mind:

*   **Read-Only Access:** Ensure the Databricks token used belongs to a service principal with `SELECT` permissions only. 
*   **The "Forbidden" Filter:** The script includes a hardcoded security check that blocks common write-operation keywords (`DROP`, `DELETE`, `UPDATE`, etc.) at the application level.
*   **Data Privacy:** If your tables contain PII (Personally Identifiable Information), ensure your workspace has dynamic data masking enabled, as the "Data Sampling" feature pulls actual values into the LLM context.
*   **Prompt Injection:** The system uses OpenAI Function Calling, which is significantly more resilient to prompt injection than simple string-interpolation methods.

---

### Key Features
*   **Vector Search:** Powered by `text-embedding-3-small`.
*   **LLM Engine:** Optimized for `gpt-4o`.
*   **Metadata:** Uses `SHOW CREATE TABLE` (DDL) to ensure the LLM understands Primary and Foreign Key relationships for accurate joins.
*   **HTML UI Ready:** Responses are pre-formatted with `<h3>`, `<p>`, and `<ul>` tags for easy rendering in web dashboards.

---

### Troubleshooting & Success Tips
*   **Join Logic:** For complex joins to work, ensure your DDL includes Key definitions. If your database doesn't have them defined, you can manually add relationship hints to the "Synonyms" generation prompt.
*   **Context Window:** If you have hundreds of tables, the `k=5` parameter in the search function ensures the prompt stays small and costs remain low.
*   **Initial Indexing:** The first run involves an AI call for every table to generate synonyms. On large schemas, this can take a few minutes but only occurs once per session.

---

**Ready to get started?** Simply update the `Configuration` section at the top of the script with your OpenAI and Databricks credentials.
