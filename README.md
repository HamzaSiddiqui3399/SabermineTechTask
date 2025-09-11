# Forward Deployed Engineer Tech Test

This template repository contains the technical test environment. Any work on this technical task should be done as if you were working in a team as a part of your real job.

### Prerequisites
- Python 3.7+
- Git
- Docker & Docker Compose
- GitHub account

## Setup Instructions

### Step 1: Create Your Repository
1. **Use this template** by clicking the "Use this template" button at the top of this page
2. Create a new repository in your GitHub account
3. Make sure to set it as **Private** initially to keep your solution confidential

### Step 2: Setup Your Environment

#### Option 1: Automated Setup (Recommended)

**For macOS/Linux:**
1. Clone **your new repository**:
   ```bash
   git clone <your_repository_url>
   ```

2. Navigate into the directory:
   ```bash
   cd Saber-FDE-Technical-Task
   ```

3. Set up a virtual environment:
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

4. Run the setup script:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

**For Windows:**
1. Clone **your new repository**:
   ```cmd
   git clone <your_repository_url>
   ```

2. Navigate into the directory:
   ```cmd
   cd Saber-FDE-Technical-Task
   ```

3. Set up a virtual environment:
    ```cmd
    python -m venv .venv
    .venv\Scripts\activate
    ```

4. Run the setup script:
   ```cmd
   setup.bat
   ```

#### Option 2: Manual Setup
1. Clone **your new repository**:
   ```bash
   git clone <your_repository_url>
   ```

2. Navigate into the directory:
   ```bash
   cd Saber-FDE-Technical-Task
   ```

3. Set up a virtual environment:
    
    **macOS/Linux:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```
    
    **Windows:**
    ```cmd
    python -m venv .venv
    .venv\Scripts\activate
    ```

4. Install Python dependencies:
   ```bash
   pip install -r api/requirements.txt
   ```

5. Build and start the environment:
   ```bash
   docker-compose up -d --build
   ```

The API server will now be running and accessible on `http://localhost:5000`.

## Verification
To verify that the environment is running correctly:

1. **Check API documentation**: Navigate to http://localhost:5000/docs

## Your Task
All instructions for the task are located in the `TASK.md` file.

## Submission Instructions
1. Complete your solution in your repository
2. When ready to submit,
   - Make your repository **Public**, OR
   - Add the reviewer as a collaborator to your private repository
3. Submit the link to your repository

## Troubleshooting
- If you encounter dependency issues, ensure you have Python 3.7+ installed
- If Docker containers fail to start, check that port 5000 is not already in use
- Run `docker-compose logs` to see detailed error messages
- If there are any problems preventing you from completing the technical test, please reach out to us