# Audience Manager Platform

This repository contains the source code for the Audience Manager Platform, a powerful tool for creating and managing audience segments based on user transaction data. The platform features a React-based frontend, a Flask backend API, and leverages Apache Spark for efficient, large-scale data processing.

---

## 📚 Full Documentation

For a complete technical overview, including system architecture, database schema, API reference, and more, please see the comprehensive documentation:

**[➡️ View Full Project Documentation](./DOCUMENTATION.md)**

---

## 🚀 Getting Started

This project is composed of two main parts: a backend service and a frontend application. Follow the instructions below to get them running locally.

### Prerequisites

-   Python 3.11+
-   Node.js v18+
-   An Apache Spark installation (the application is configured to find `spark-submit` in your system's PATH).

### 1. Backend Setup

Navigate to the backend directory and follow its specific setup instructions:

```bash
cd backend
# Follow instructions in backend/README.md
FLASK_ENV=development flask run  
```

The backend server will start on `http://127.0.0.1:5000`.
