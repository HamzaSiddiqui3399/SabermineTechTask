# Bespoke Engineering Technical Task

### The Scenario
You are a Forward Deployed Engineer. A client has provided you with a database of their customers and recent orders. They need you to enrich this data using their "Social Profile API" and submit the results for an upcoming marketing campaign.

### The Goal
Write a Python script that enriches customer data using the API, then pushes the updated rows to the customer's system using their API. You should produce a CSV file showing all of the enriched data, and the results of the attempts to push to the client's system

### Rules
You are **not** allowed to change any of the code in the database and api folders. This code sets up the simulated environment and **should not** be touched.

### Requirements
1.  **Query the Database:** From the `client_data.db`, select all customers located in **'Manchester'** who have placed an order in the last year (since 1st September 2024 00:00:00).
See `DATABASE.md` for information about the DB Schema. You will need to extract the following information from the database:
    - `customer_id`
    - `name`
    - `email`
    - `total_spend`

2.  **Enrich the Data:** For each of these customers, use their email to call the Enrichment API (`GET http://localhost:5000/enrichment`). This will return you the customer's `social_handle`
3.  **Handle API Errors:** The API is unreliable. Your script **must** be resilient to:
    * Rate limiting.
    * Random server error.
    * Missing profiles.
    * Authentication is required (`X-API-KEY: SECRET_KEY_123`).
4. **Push the Data:** For each row of enriched data, push the data to the API. Each row should conform to the requirements of the API.
    This push will be subject to the same unreliable API conditions, which your script **must** be resilient to.

5.  **Generate the Final CSV:** The final output should be a CSV file with the following columns:
    ```
    customer_id,name,email,total_spend,social_handle,success,reason
    ```
    Each row should contain the enriched customer data, for example:
    ```csv
    customer_id,name,email,total_spend,social_handle,true,reason
    A102,Jane Doe,jane.doe@example.com,850.75,@jane_d_social,true,reason
    B203,John Smith,john.smith@example.com,1250.30,@john_smith_social,false,reason
    ```
### What We're Looking For
* **Correctness:** Does the script produce the right output?
* **Robustness:** How gracefully does your script handle messy data and API failures? This is critical.
* **Readability:** Is your code well structured and easy to understand?
* **Logging:** Your script should produce meaningful logs and results so that if a record fails, we know which one and why.

### Final Submission
Please send a URL to your finished GitHub Repository with your script included, and final output csv included in the repository.