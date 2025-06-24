# Backend Service

This directory contains the Flask-based backend API for the Audience Manager Platform.

---

## API Documentation

For a detailed breakdown of all available API endpoints, request/response formats, and usage examples, please refer to the main project documentation:

**[➡️ View API Reference](../../DOCUMENTATION.md#backend-api-reference)**

---

## Local Setup

Follow these steps to get the backend service running on your local machine.

### 1. Create a Virtual Environment

From within this `backend` directory, create and activate a Python virtual environment. This will keep the project's dependencies isolated.

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Dependencies

Install all the required Python packages using the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

### 3. Initialize the Database

Before running the application for the first time, you need to create the SQLite database and all the necessary tables. The following command will create the `db/audience_manager.db` file and set up the schema.

```bash
python3 -m flask db upgrade
```

*Note: This command uses Flask-Migrate, which we've set up to manage our database schema. It's the correct way to initialize the database based on our models.*

### 4. Seed the Database (Optional but Recommended)

To populate the application with realistic mock data for testing, run the seed script:

```bash
python3 seed_database.py
```

This will create users and several thousand transactions, which is necessary for testing the rule engine and segmentation features.

### 5. Run the Server

Finally, start the Flask development server.

```bash
python3 run.py
```

The API will now be running and accessible at `http://127.0.0.1:5000`.
