# Event Lens

## Application Links

- **Airflow**: http://34.230.153.81:8080/
- **Streamlit Application**: http://34.230.153.81:8501/
- **CodeLabs**: https://codelabs-preview.appspot.com/?file_id=1DX_AJcObbNu1kcJfB7VmxRtHt1wsTYe33v6MJVLe2X0/edit?tab=t.0#0
- **Youtube Video**: https://youtu.be/o8XeGBa-4OE

---

## Problem Statement

Boston hosts hundreds of events every week — from local meetups and college fests to cultural shows and community pop-ups. However, no single platform brings all of these together.

Most existing platforms prioritize promoted or paid events, leaving many free or community-based events buried across websites or social media platforms like Instagram. Users often miss out due to scattered sources and lack of centralized access.

### Proposed Solution

**Event Lens** is a Boston-focused event discovery platform designed to help users find local events tailored to their interests. It aggregates data from trusted websites and Instagram pages to showcase a diverse range of events — from concerts to college fests and free gatherings.

Key features include:

- Natural language chatbot for intuitive search
- Detailed event pages with descriptions, timing, cost, location, and weather forecasts
- Real reviews and navigation support

**Event Lens** aims to be the go-to app for discovering what’s happening in and around Boston.

### Data Sources

We pull data from the following sources:

- [The Boston Calendar](https://www.thebostoncalendar.com/)
- [Boston.gov](https://www.boston.gov/)
- [Meet Boston](https://www.meetboston.com/events/)
- [Boston Central](https://www.bostoncentral.com/)
- Instagram Pages:
  - Boston Events Guide
  - Boston Today
  - Boston Parks Dept
  - Boston Desi Events

---

## Project Goals

1. **Comprehensive Event Aggregation**: Aggregate a wide variety of events from multiple platforms.
2. **Real-Time Updates**: Ensure fresh and timely information on ongoing and upcoming events.
3. **Personalized Recommendations**: Leverage user preferences to recommend relevant events.
4. **Scalable Data Handling**: Efficiently manage and process large volumes of scraped data.
5. **Duplicate Elimination**: Use similarity checks to prevent redundant event entries.

---

## Technologies Used

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) 
![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white) 
![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup-61DAFB?style=for-the-badge&logo=python&logoColor=black)
![Apify](https://img.shields.io/badge/Apify-FF8A00?style=for-the-badge&logo=apify&logoColor=white) 
![OpenAI](https://img.shields.io/badge/OpenAI-4E8EE9?style=for-the-badge&logo=openai&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-00B0D6?style=for-the-badge&logo=snowflake&logoColor=white) 
![Airflow](https://img.shields.io/badge/Airflow-017D4F?style=for-the-badge&logo=apache-airflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white) 
![LangGraph](https://img.shields.io/badge/LangGraph-00BFFF?style=for-the-badge&logo=python&logoColor=white)
![SERP API](https://img.shields.io/badge/SERP_API-00B0D6?style=for-the-badge&logo=google&logoColor=white) 
![Weather API](https://img.shields.io/badge/Weather_API-1E90FF?style=for-the-badge&logo=weather&logoColor=white) 
![Google Maps API](https://img.shields.io/badge/Google_Maps_API-F44336?style=for-the-badge&logo=google-maps&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=for-the-badge&logo=amazonaws&logoColor=white) 
![AWS EC2](https://img.shields.io/badge/AWS_EC2-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)


---

## Architecture

### Data Pipeline

![image](https://github.com/user-attachments/assets/d5507952-6f0b-4dec-a544-42567fe4a8d6)

1. **Data Scraping**Scrape events from websites (e.g., BostonCentral.com, The Boston Calendar) using Selenium and BeautifulSoup. Instagram posts are fetched using Apify API.
2. **Data Transformation**Structure and clean the data using Python scripts before storing it in staging.
3. **Image Upload**Upload associated images to Amazon S3.
4. **Snowflake Staging**Load transformed data into Snowflake staging tables.
5. **Snowflake Cortex Processing**Use Snowflake Cortex models to:

   - Classify and complete event data
   - Extract key information (e.g., duration, category)
   - Generate vector embeddings for similarity matching
6. **Similarity Check**

   - If similarity < 90% → Insert into Snowflake EDW
   - If similarity ≥ 90% → Discard to avoid duplication
7. **Metrics Capture**
   Track and store pipeline job metrics in Snowflake.

### Langgraph Architecture

![image](https://github.com/user-attachments/assets/d6ff7adb-4c4f-462b-8628-e91b7c4fb9fa)

The Langgraph Architecture is as follows:

1. **Relevance Check (OpenAI)**:
   a. The process begins when a user inputs a query. The LangGraph Controller accepts the query and initiates the Relevance Check.
   b. The Relevance Check determines whether the query is relevant to the event data or any of the available tools (e.g., weather, sentiment analysis, location, etc.). It uses OpenAI to classify the query and check its relevance to the context of the system's capabilities.

2. **LangGraph Controller (Agent)**:
   a. If the query is deemed relevant, the Controller Agent then decides the appropriate tools or nodes to process the query.
   b. The controller directs the query to the necessary functions, depending on the identified context. The controller also manages the flow of information, routing the query to either single or multiple tools as needed.
   c. Based on the query type, the Controller Node directs the query to the appropriate tool. The possible tools include:

   - **Weather**: It routes the request to the OpenWeather API for weather-related queries.
   - **Sentiment**: If the query is about sentiment, it sends the request to the SERP API for sentiment analysis.
   - **Location**: For location-based queries, it connects to the Google Maps API to retrieve the necessary geographical data.
   - **Complex Questions**: For more complex queries, it utilizes the Snowflake Cortex models, specifically the RAG-based COT API, to process the data and answer the query.

3. **External APIs**:

   - **Weather API (OpenWeather)**: Fetches data related to the weather based on the query. Given an NLP query, we have another LLM to fetch the relevant information to call the API.
   - **SERP API (Sentiment Analysis)**: Analyzes the sentiment in search engine results, helping to understand the tone of the query.
   - **Google Maps API**: Retrieves information related to geographical locations based on the user's query. From the natural language query, we extract the information required for the API and which is being sent to the Google Maps API.
   - **RAG-based COT API (Snowflake)**: Used for complex queries that need data analysis from Snowflake's Cortex models, particularly useful for deep data searches.

4. **Final Answer Node (OpenAI)**:
   a. Once the necessary data is gathered from the external APIs, the Final Answer Node processes the gathered information and generates a coherent response.
   b. This response is then sent back to the user, completing the query-response cycle.

---

### Frontend Application
![image](https://github.com/user-attachments/assets/920ccde4-71f9-4a2c-bed5-29b597c1e4f1)

#### Streamlit

- Allows users to:
  - Search for events
  - Chat with the event chatbot
  - View event recommendations

#### FastAPI

- Acts as the backend service for:
  - User authentication (JWT-based)
  - Event search and image fetch from S3
  - Recommendation delivery
  - Integration with Snowflake via COT RAG and Cortex APIs

#### LangGraph Chatbot API

- Enables natural language interaction to guide users to relevant events.

#### GitHub Actions (CI/CD)

- Automates:
  - Docker build and deployment
  - Continuous integration for frontend/backend

---

## How to Run the Application

### Airflow

1. **Clone the Repository**

   ```bash
   git clone https://github.com/Gen-AI-with-LLM-Spring2025-Team6/EventLens.git
   ```
2. **Navigate to Parameters Directory**

   ```bash
   cd EventLens/airflow/data_load/parameters
   ```
3. **Set Environment Variables**Create a `.env` file:

   ```env
   SNOWFLAKE_ACCOUNT=your_snowflake_account
   SNOWFLAKE_USER=your_snowflake_user
   SNOWFLAKE_PASSWORD=your_snowflake_password
   SNOWFLAKE_DATABASE=your_snowflake_database
   SNOWFLAKE_SCHEMA=your_snowflake_schema
   SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
   SNOWFLAKE_ROLE=your_snowflake_role
   FAST_API_URL=your_fast_api_url
   OPENAI_API_KEY=your_openai_api_key
   ```
4. **Build Docker Image**

   ```bash
   docker build -t boston-calendar-airflow .
   ```
5. **Run Docker Container**

   ```bash
   docker run -d -p 8080:8080 boston-calendar-airflow
   ```
6. **Access Airflow UI**Visit `http://localhost:8080`Default Credentials:

   - Username: `admin`
   - Password: `admin`

---

### Streamlit + FastAPI

Steps:

1. Clone the repository
2. Navigate inside the `EventLens` directory
3. Set up environment variables in `.env` file
4. Build Docker images
5. Run Docker containers
6. Access apps at:
   - **Streamlit**: [http://localhost:8501](http://localhost:8501)
   - **FastAPI Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Deployment

Deployment is fully automated via **GitHub Actions**.

### Steps:

1. **Trigger**: Push to the main branch (README changes are ignored).
2. **Checkout Code**: Pull the latest repository version.
3. **Set Up Tools**: Initialize QEMU and Docker Buildx for cross-platform builds.
4. **Docker Login**: Use GitHub secrets for Docker Hub credentials.
5. **Build & Push Image**:
   ```bash
   docker buildx build --platform linux/arm64 -t <docker-image>:latest --push .
   ```
6. **Deploy via SSH to EC2**:
   - Stop old containers
   - Pull new image
   - Launch updated app with env variables
7. **Environment Secrets**: Managed securely through GitHub Secrets.

---

## Future Scope

- **City Expansion**: Cover more cities beyond Boston.
- **User-Submitted Events**: Allow users to submit and manage their events.
- **Advanced Personalization**: ML-based recommendation engine.
- **Calendar Sync**: Add events to personal calendars.
- **Ticketing Integration**: Support ticket purchases within the app.
- **RSVP System**: Enable notifications and RSVPs.

---

## Conclusion

**Event Lens** solves the often overlooked issue of discovering local events in a centralized, personalized, and accessible way. By combining automation, AI, and thoughtful design, Event Lens ensures that users never miss what matters around them.

---

## References

- [Beautiful Soup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Diagrams Tool](https://diagrams.mingrammer.com/)
- [Google Cloud](https://cloud.google.com)
- [Snowflake](https://www.snowflake.com)
